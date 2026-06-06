package transactions

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// IDTotal is a per-entity money rollup returned by grouped sum aggregations.
type IDTotal struct {
	ID    bson.ObjectID `bson:"_id"`
	Total float64       `bson:"total"`
	Count int           `bson:"count"`
}

// idRange builds an _id range filter over (since, until] from ObjectID timestamps.
func idRange(since, until time.Time) bson.D {
	return bson.D{{Key: "_id", Value: bson.D{
		{Key: "$gt", Value: bson.NewObjectIDFromTimestamp(since)},
		{Key: "$lte", Value: bson.NewObjectIDFromTimestamp(until)},
	}}}
}

// sumMoney sums the money field of docs in (since, until].
func sumMoney(ctx context.Context, coll *mongo.Collection, since, until time.Time) (float64, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: idRange(since, until)}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$money"}}},
		}}},
	}
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, err
	}
	defer cursor.Close(ctx)

	var rows []struct {
		Total float64 `bson:"total"`
	}
	err = cursor.All(ctx, &rows)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return rows[0].Total, nil
}

// sumMoneyByField groups money by a field over (since, until].
func sumMoneyByField(ctx context.Context, coll *mongo.Collection, field string, since, until time.Time) ([]IDTotal, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: idRange(since, until)}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$" + field},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$money"}}},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	}
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []IDTotal
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// WageStats is the weighted wage-rate summary over a window.
type WageStats struct {
	Money       float64
	Volume      int
	Count       int
	MinRate     float64
	MaxRate     float64
	WeightedAvg float64
}

// AggregateStats returns weighted wage-rate stats over (since, until].
func (s *WageTransactionStore) AggregateStats(ctx context.Context, since, until time.Time) (WageStats, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "$gt", Value: bson.NewObjectIDFromTimestamp(since)},
				{Key: "$lte", Value: bson.NewObjectIDFromTimestamp(until)},
			}},
			{Key: "quantity", Value: bson.D{{Key: "$gt", Value: 0}}},
		}}},
		{{Key: "$addFields", Value: bson.D{
			{Key: "rate", Value: bson.D{{Key: "$divide", Value: bson.A{"$money", "$quantity"}}}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "money", Value: bson.D{{Key: "$sum", Value: "$money"}}},
			{Key: "volume", Value: bson.D{{Key: "$sum", Value: "$quantity"}}},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
			{Key: "minRate", Value: bson.D{{Key: "$min", Value: "$rate"}}},
			{Key: "maxRate", Value: bson.D{{Key: "$max", Value: "$rate"}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return WageStats{}, err
	}
	defer cursor.Close(ctx)

	var rows []struct {
		Money   float64 `bson:"money"`
		Volume  int     `bson:"volume"`
		Count   int     `bson:"count"`
		MinRate float64 `bson:"minRate"`
		MaxRate float64 `bson:"maxRate"`
	}
	err = cursor.All(ctx, &rows)
	if err != nil {
		return WageStats{}, err
	}
	if len(rows) == 0 {
		return WageStats{}, nil
	}
	r := rows[0]
	avg := 0.0
	if r.Volume > 0 {
		avg = r.Money / float64(r.Volume)
	}
	return WageStats{
		Money:       r.Money,
		Volume:      r.Volume,
		Count:       r.Count,
		MinRate:     r.MinRate,
		MaxRate:     r.MaxRate,
		WeightedAvg: avg,
	}, nil
}

// TopPaidEmployees returns employees ranked by total money received in (since, until].
func (s *WageTransactionStore) TopPaidEmployees(ctx context.Context, since, until time.Time, limit int, ascending bool) ([]IDTotal, error) {
	order := -1
	if ascending {
		order = 1
	}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: idRange(since, until)}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$employeeId"},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$money"}}},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "total", Value: order}}}},
		{{Key: "$limit", Value: limit}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []IDTotal
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// EarnedByEmployee groups wage money received per employee over (since, until].
func (s *WageTransactionStore) EarnedByEmployee(ctx context.Context, since, until time.Time) ([]IDTotal, error) {
	return sumMoneyByField(ctx, s.coll, "employeeId", since, until)
}

// PaidByEmployer groups wage money paid per employer over (since, until].
func (s *WageTransactionStore) PaidByEmployer(ctx context.Context, since, until time.Time) ([]IDTotal, error) {
	return sumMoneyByField(ctx, s.coll, "employerId", since, until)
}

// WageRow is a single wage payment with its derived employee/company context.
type WageRow struct {
	ID         bson.ObjectID `bson:"_id"`
	EmployeeID bson.ObjectID `bson:"employeeId"`
	EmployerID bson.ObjectID `bson:"employerId"`
	Money      float64       `bson:"money"`
}

// GetRange returns wage payments in (since, until] ordered by _id ascending.
func (s *WageTransactionStore) GetRange(ctx context.Context, since, until time.Time) ([]WageRow, error) {
	cursor, err := s.coll.Find(ctx, idRange(since, until),
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []WageRow
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SumMoney sums trade money over (since, until].
func (s *TradeTransactionStore) SumMoney(ctx context.Context, since, until time.Time) (float64, error) {
	return sumMoney(ctx, s.coll, since, until)
}

// DistinctItemCodes returns the set of fungible item codes ever traded.
func (s *TradeTransactionStore) DistinctItemCodes(ctx context.Context) ([]string, error) {
	res := s.coll.Distinct(ctx, "itemCode", bson.D{})
	err := res.Err()
	if err != nil {
		return nil, err
	}
	var out []string
	err = res.Decode(&out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetRange returns trade transactions in (since, until] ordered by _id ascending.
func (s *TradeTransactionStore) GetRange(ctx context.Context, since, until time.Time) ([]TradeTransaction, error) {
	cursor, err := s.coll.Find(ctx, idRange(since, until),
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []TradeTransaction
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MoneyByField groups trade money by a chosen field over (since, until].
func (s *TradeTransactionStore) MoneyByField(ctx context.Context, field string, since, until time.Time) ([]IDTotal, error) {
	return sumMoneyByField(ctx, s.coll, field, since, until)
}

// SumMoney sums equipment-market money over (since, until].
func (s *MarketTransactionStore) SumMoney(ctx context.Context, since, until time.Time) (float64, error) {
	return sumMoney(ctx, s.coll, since, until)
}

// GetRange returns equipment-market transactions in (since, until] ordered by _id ascending.
func (s *MarketTransactionStore) GetRange(ctx context.Context, since, until time.Time) ([]MarketTransaction, error) {
	cursor, err := s.coll.Find(ctx, idRange(since, until),
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []MarketTransaction
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// EquipmentTrade is one equipment sale joined with its item attributes.
type EquipmentTrade struct {
	ID       bson.ObjectID      `bson:"_id"`
	ItemCode string             `bson:"itemCode"`
	Skills   map[string]float64 `bson:"skills"`
	Money    float64            `bson:"money"`
	At       time.Time          `bson:"at"`
}

// GetEquipmentTradesRange returns equipment sales in (since, until] joined to the items tracker.
func (s *MarketTransactionStore) GetEquipmentTradesRange(ctx context.Context, since, until time.Time) ([]EquipmentTrade, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: idRange(since, until)}},
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "items"},
			{Key: "localField", Value: "itemId"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "item"},
		}}},
		{{Key: "$unwind", Value: "$item"}},
		{{Key: "$project", Value: bson.D{
			{Key: "itemCode", Value: "$item.itemCode"},
			{Key: "skills", Value: "$item.skills"},
			{Key: "money", Value: "$money"},
			{Key: "at", Value: bson.D{{Key: "$toDate", Value: "$_id"}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []EquipmentTrade
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CaseDrop is one case opening joined with the dropped item's attributes.
type CaseDrop struct {
	ID       bson.ObjectID      `bson:"_id"`
	UserID   bson.ObjectID      `bson:"userId"`
	Case     string             `bson:"case"`
	ItemCode string             `bson:"itemCode"`
	Skills   map[string]float64 `bson:"skills"`
	At       time.Time          `bson:"at"`
}

// GetDropsRange returns case openings in (since, until] joined to the items tracker.
func (s *CaseTransactionStore) GetDropsRange(ctx context.Context, since, until time.Time) ([]CaseDrop, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: idRange(since, until)}},
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "items"},
			{Key: "localField", Value: "itemId"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "item"},
		}}},
		{{Key: "$unwind", Value: "$item"}},
		{{Key: "$project", Value: bson.D{
			{Key: "userId", Value: "$userId"},
			{Key: "case", Value: "$case"},
			{Key: "itemCode", Value: "$item.itemCode"},
			{Key: "skills", Value: "$item.skills"},
			{Key: "at", Value: bson.D{{Key: "$toDate", Value: "$_id"}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []CaseDrop
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// EarliestTime returns the oldest case transaction's time and whether any exist.
func (s *CaseTransactionStore) EarliestTime(ctx context.Context) (time.Time, bool, error) {
	return earliestObjectIDTime(ctx, s.coll)
}

// CountByUser counts case openings per user over (since, until].
func (s *CaseTransactionStore) CountByUser(ctx context.Context, since, until time.Time) ([]IDTotal, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: idRange(since, until)}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$userId"},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []IDTotal
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DismantleRow is one dismantle joined with the destroyed item's code and state.
type DismantleRow struct {
	ID       bson.ObjectID `bson:"_id"`
	UserID   bson.ObjectID `bson:"userId"`
	ItemCode string        `bson:"itemCode"`
	State    int           `bson:"state"`
	At       time.Time     `bson:"at"`
}

// GetRange returns dismantles in (since, until] joined to the items tracker.
func (s *DismantleTransactionStore) GetRange(ctx context.Context, since, until time.Time) ([]DismantleRow, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: idRange(since, until)}},
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "items"},
			{Key: "localField", Value: "itemId"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "item"},
		}}},
		{{Key: "$unwind", Value: "$item"}},
		{{Key: "$project", Value: bson.D{
			{Key: "userId", Value: "$userId"},
			{Key: "itemCode", Value: "$item.itemCode"},
			{Key: "state", Value: "$item.state"},
			{Key: "at", Value: bson.D{{Key: "$toDate", Value: "$_id"}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []DismantleRow
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// EarliestTime returns the oldest dismantle transaction's time and whether any exist.
func (s *DismantleTransactionStore) EarliestTime(ctx context.Context) (time.Time, bool, error) {
	return earliestObjectIDTime(ctx, s.coll)
}

// MoneyByField groups equipment-market money by a chosen field over (since, until].
func (s *MarketTransactionStore) MoneyByField(ctx context.Context, field string, since, until time.Time) ([]IDTotal, error) {
	return sumMoneyByField(ctx, s.coll, field, since, until)
}
