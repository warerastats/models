package transactions

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type WageTransaction struct {
	ID               bson.ObjectID `bson:"_id"`
	EmployeeID       bson.ObjectID `bson:"employeeId"`
	EmployerID       bson.ObjectID `bson:"employerId"`
	Money            float64       `bson:"money"`
	ProductionPoints int           `bson:"quantity"`
}

type WageTransactionStore struct {
	coll *mongo.Collection
}

func NewWageTransactionStore(ctx context.Context, db *mongo.Database) *WageTransactionStore {
	store := &WageTransactionStore{
		coll: db.Collection("wage_transactions"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *WageTransactionStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "employeeId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on wage_transactions.employeeId",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "employerId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on wage_transactions.employerId",
			"error", err,
		)
		return
	}
}

func (s *WageTransactionStore) Create(
	ctx context.Context,
	id bson.ObjectID,
	employeeID bson.ObjectID,
	employerID bson.ObjectID,
	money float64,
	productionPoints int,
) error {
	tx := WageTransaction{
		ID:               id,
		EmployeeID:       employeeID,
		EmployerID:       employerID,
		Money:            money,
		ProductionPoints: productionPoints,
	}

	_, err := s.coll.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$setOnInsert", Value: tx}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

// BulkCreate inserts a batch of transactions in a single unordered bulk write.
// Each op is an idempotent upsert keyed on _id with $setOnInsert, so
// re-ingesting an already-stored transaction is a no-op.
func (s *WageTransactionStore) BulkCreate(ctx context.Context, txs []WageTransaction) error {
	if len(txs) == 0 {
		return nil
	}
	ops := make([]mongo.WriteModel, len(txs))
	for i := range txs {
		ops[i] = mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: txs[i].ID}}).
			SetUpdate(bson.D{{Key: "$setOnInsert", Value: txs[i]}}).
			SetUpsert(true)
	}
	_, err := s.coll.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	return err
}

// DistinctEmployees returns the set of employeeIds appearing in
// wage_transactions whose _id falls in (since, until]. Range is computed
// from ObjectID timestamps so no extra index is needed.
func (s *WageTransactionStore) DistinctEmployees(ctx context.Context, since, until time.Time) ([]bson.ObjectID, error) {
	minID := bson.NewObjectIDFromTimestamp(since)
	maxID := bson.NewObjectIDFromTimestamp(until)

	filter := bson.D{{Key: "_id", Value: bson.D{
		{Key: "$gt", Value: minID},
		{Key: "$lte", Value: maxID},
	}}}

	res := s.coll.Distinct(ctx, "employeeId", filter)
	err := res.Err()
	if err != nil {
		return nil, err
	}

	var ids []bson.ObjectID
	err = res.Decode(&ids)
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// WageCandleBucket is one OHLC accumulator row from AggregateWageCandles (avg = money/volume).
type WageCandleBucket struct {
	BucketStart time.Time `bson:"_id"`
	Open        float64   `bson:"open"`
	High        float64   `bson:"high"`
	Low         float64   `bson:"low"`
	Close       float64   `bson:"close"`
	Volume      int       `bson:"volume"`
	Money       float64   `bson:"money"`
	Count       int       `bson:"count"`
}

// AggregateWageCandles buckets wage payments in (since, until] into OHLC windows of binSizeMinutes.
func (s *WageTransactionStore) AggregateWageCandles(ctx context.Context, since, until time.Time, binSizeMinutes int) ([]WageCandleBucket, error) {
	minID := bson.NewObjectIDFromTimestamp(since)
	maxID := bson.NewObjectIDFromTimestamp(until)

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "_id", Value: bson.D{{Key: "$gt", Value: minID}, {Key: "$lte", Value: maxID}}},
			{Key: "quantity", Value: bson.D{{Key: "$gt", Value: 0}}},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$addFields", Value: bson.D{
			{Key: "unit", Value: bson.D{{Key: "$divide", Value: bson.A{"$money", "$quantity"}}}},
			{Key: "bucketStart", Value: bson.D{{Key: "$dateTrunc", Value: bson.D{
				{Key: "date", Value: bson.D{{Key: "$toDate", Value: "$_id"}}},
				{Key: "unit", Value: "minute"},
				{Key: "binSize", Value: binSizeMinutes},
			}}}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$bucketStart"},
			{Key: "open", Value: bson.D{{Key: "$first", Value: "$unit"}}},
			{Key: "close", Value: bson.D{{Key: "$last", Value: "$unit"}}},
			{Key: "high", Value: bson.D{{Key: "$max", Value: "$unit"}}},
			{Key: "low", Value: bson.D{{Key: "$min", Value: "$unit"}}},
			{Key: "money", Value: bson.D{{Key: "$sum", Value: "$money"}}},
			{Key: "volume", Value: bson.D{{Key: "$sum", Value: "$quantity"}}},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	}

	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []WageCandleBucket
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// EarliestTime returns the oldest wage transaction's time and whether any exist.
func (s *WageTransactionStore) EarliestTime(ctx context.Context) (time.Time, bool, error) {
	return earliestObjectIDTime(ctx, s.coll)
}

// ByUser returns wage payments where the user is the employee or employer,
// newest first. Pass a non-nil before (an _id) to page: only documents with
// _id < before are returned. _id encodes creation time, so it doubles as the
// time-ordered pagination cursor.
func (s *WageTransactionStore) ByUser(ctx context.Context, userID bson.ObjectID, before *bson.ObjectID, limit int) ([]WageTransaction, error) {
	if limit <= 0 {
		limit = 20
	}
	filter := bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "employeeId", Value: userID}},
		bson.D{{Key: "employerId", Value: userID}},
	}}}
	if before != nil {
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$lt", Value: *before}}})
	}
	cursor, err := s.coll.Find(ctx, filter,
		options.Find().
			SetSort(bson.D{{Key: "_id", Value: -1}}).
			SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []WageTransaction
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
