package transactions

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type TradeTransaction struct {
	ID              bson.ObjectID  `bson:"_id"`
	SellerID        bson.ObjectID  `bson:"sellerId"`
	BuyerID         bson.ObjectID  `bson:"buyerId"`
	SellerMuID      *bson.ObjectID `bson:"sellerMuId,omitempty"`
	BuyerMuID       *bson.ObjectID `bson:"buyerMuId,omitempty"`
	SellerCountryID *bson.ObjectID `bson:"sellerCountryId,omitempty"`
	BuyerCountryID  *bson.ObjectID `bson:"buyerSellerId,omitempty"`
	ItemOfferID     *bson.ObjectID `bson:"itemOfferId,omitempty"`
	ItemCode        string         `bson:"itemCode"`
	Money           float64        `bson:"money"`
	Quantity        int            `bson:"quantity"`
	// In ms
	TimeTillSale int64 `bson:"tts"`
}

type TradeTransactionStore struct {
	coll *mongo.Collection
}

func NewTradeTransactionStore(ctx context.Context, db *mongo.Database) *TradeTransactionStore {
	store := &TradeTransactionStore{
		coll: db.Collection("trade_transactions"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *TradeTransactionStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "sellerId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on trade_transactions.sellerId",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "buyerId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on trade_transactions.buyerId",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "itemCode", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on trade_transactions.itemCode",
			"error", err,
		)
		return
	}
}

func (s *TradeTransactionStore) Create(
	ctx context.Context,
	id bson.ObjectID,
	sellerID bson.ObjectID,
	buyerID bson.ObjectID,
	sellerMuID *bson.ObjectID,
	buyerMuID *bson.ObjectID,
	sellerCountryID *bson.ObjectID,
	buyerCountryID *bson.ObjectID,
	itemOfferID *bson.ObjectID,
	itemCode string,
	money float64,
	quantity int,
	timeTillSale int64,
) error {
	tx := TradeTransaction{
		ID:              id,
		SellerID:        sellerID,
		BuyerID:         buyerID,
		SellerMuID:      sellerMuID,
		BuyerMuID:       buyerMuID,
		SellerCountryID: sellerCountryID,
		BuyerCountryID:  buyerCountryID,
		ItemOfferID:     itemOfferID,
		ItemCode:        itemCode,
		Money:           money,
		Quantity:        quantity,
		TimeTillSale:    timeTillSale,
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
func (s *TradeTransactionStore) BulkCreate(ctx context.Context, txs []TradeTransaction) error {
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

// ItemCandleBucket is one OHLC accumulator row from AggregateItemCandles (avg = money/volume).
type ItemCandleBucket struct {
	Key struct {
		ItemCode    string    `bson:"itemCode"`
		BucketStart time.Time `bson:"bucketStart"`
	} `bson:"_id"`
	Open   float64 `bson:"open"`
	High   float64 `bson:"high"`
	Low    float64 `bson:"low"`
	Close  float64 `bson:"close"`
	Volume int     `bson:"volume"`
	Money  float64 `bson:"money"`
	Count  int     `bson:"count"`
}

// AggregateItemCandles buckets trades in (since, until] into per-item OHLC windows of binSizeMinutes.
func (s *TradeTransactionStore) AggregateItemCandles(ctx context.Context, since, until time.Time, binSizeMinutes int) ([]ItemCandleBucket, error) {
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
			{Key: "_id", Value: bson.D{
				{Key: "itemCode", Value: "$itemCode"},
				{Key: "bucketStart", Value: "$bucketStart"},
			}},
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

	var out []ItemCandleBucket
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// EarliestTime returns the oldest trade transaction's time and whether any exist.
func (s *TradeTransactionStore) EarliestTime(ctx context.Context) (time.Time, bool, error) {
	return earliestObjectIDTime(ctx, s.coll)
}
