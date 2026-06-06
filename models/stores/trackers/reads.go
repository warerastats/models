package trackers

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// GetByBattle returns all damage rows for a battle ordered by time ascending.
func (s *DamageStore) GetByBattle(ctx context.Context, battleID bson.ObjectID) ([]Damage, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "battleId", Value: battleID}},
		options.Find().SetSort(bson.D{{Key: "at", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Damage
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// UserDamageAgg is a per-user damage rollup across a time window.
type UserDamageAgg struct {
	UserID      bson.ObjectID `bson:"_id"`
	TotalDamage int64         `bson:"totalDamage"`
	BattleCount int           `bson:"battleCount"`
}

// AggregateUserDamage rolls up damage per user over (since, until] by the at field.
func (s *DamageStore) AggregateUserDamage(ctx context.Context, since, until time.Time) ([]UserDamageAgg, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "at", Value: bson.D{{Key: "$gt", Value: since}, {Key: "$lte", Value: until}}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$userId"},
			{Key: "totalDamage", Value: bson.D{{Key: "$sum", Value: "$damages"}}},
			{Key: "battles", Value: bson.D{{Key: "$addToSet", Value: "$battleId"}}},
		}}},
		{{Key: "$project", Value: bson.D{
			{Key: "totalDamage", Value: 1},
			{Key: "battleCount", Value: bson.D{{Key: "$size", Value: "$battles"}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []UserDamageAgg
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// EarliestDamageTime returns the time of the oldest damage row and whether any exist.
func (s *DamageStore) EarliestDamageTime(ctx context.Context) (time.Time, bool, error) {
	var doc struct {
		At time.Time `bson:"at"`
	}
	err := s.coll.FindOne(ctx, bson.D{},
		options.FindOne().SetSort(bson.D{{Key: "at", Value: 1}}).
			SetProjection(bson.D{{Key: "at", Value: 1}}),
	).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return doc.At, true, nil
}

// GetReportable returns battles that are active or ended at/after endedSince.
func (s *BattleStore) GetReportable(ctx context.Context, endedSince time.Time) ([]Battle, error) {
	filter := bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "active", Value: true}},
		bson.D{{Key: "endedAt", Value: bson.D{{Key: "$gte", Value: endedSince}}}},
	}}}
	cursor, err := s.coll.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Battle
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetMany returns battle documents for the given ids.
func (s *BattleStore) GetMany(ctx context.Context, ids []bson.ObjectID) ([]Battle, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Battle
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetRange returns damage rows in (since, until] by the at field, ascending.
func (s *DamageStore) GetRange(ctx context.Context, since, until time.Time) ([]Damage, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "at", Value: bson.D{{Key: "$gt", Value: since}, {Key: "$lte", Value: until}}}},
		options.Find().SetSort(bson.D{{Key: "at", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Damage
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetAll returns every region tracker document.
func (s *RegionStore) GetAll(ctx context.Context) ([]Region, error) {
	cursor, err := s.coll.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Region
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetAll returns every country tracker document.
func (s *CountryStore) GetAll(ctx context.Context) ([]Country, error) {
	cursor, err := s.coll.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Country
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetAll returns every company tracker document.
func (s *CompanyStore) GetAll(ctx context.Context) ([]Company, error) {
	cursor, err := s.coll.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Company
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetByUserID returns the employee document for a user, or false when none exists.
func (s *EmployeeStore) GetByUserID(ctx context.Context, userID bson.ObjectID) (*Employee, bool, error) {
	var e Employee
	err := s.coll.FindOne(ctx, bson.D{{Key: "userId", Value: userID}}).Decode(&e)
	if err == mongo.ErrNoDocuments {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &e, true, nil
}

// GetMany returns user documents for the given ids.
func (s *UserStore) GetMany(ctx context.Context, ids []bson.ObjectID) ([]User, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []User
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetActiveSince returns ids of users whose lastDate is at/after the cutoff.
func (s *UserStore) GetActiveSince(ctx context.Context, cutoff time.Time) ([]bson.ObjectID, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "lastDate", Value: bson.D{{Key: "$gte", Value: cutoff}}}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []bson.ObjectID
	for cursor.Next(ctx) {
		var r struct {
			ID bson.ObjectID `bson:"_id"`
		}
		err = cursor.Decode(&r)
		if err != nil {
			return nil, err
		}
		out = append(out, r.ID)
	}
	return out, cursor.Err()
}

// CountryAgg is a per-country rollup of member count, wealth, and damage.
type CountryAgg struct {
	CountryID   bson.ObjectID `bson:"_id"`
	MemberCount int           `bson:"memberCount"`
}

// CountByCountry returns the number of users per country.
func (s *UserStore) CountByCountry(ctx context.Context) ([]CountryAgg, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$countryId"},
			{Key: "memberCount", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []CountryAgg
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetByCountry returns ids of users belonging to a country.
func (s *UserStore) GetByCountry(ctx context.Context, countryID bson.ObjectID) ([]bson.ObjectID, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "countryId", Value: countryID}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []bson.ObjectID
	for cursor.Next(ctx) {
		var r struct {
			ID bson.ObjectID `bson:"_id"`
		}
		err = cursor.Decode(&r)
		if err != nil {
			return nil, err
		}
		out = append(out, r.ID)
	}
	return out, cursor.Err()
}

// GetMany returns item documents for the given ids.
func (s *ItemStore) GetMany(ctx context.Context, ids []bson.ObjectID) ([]Item, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Item
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// InventoryCount is a per-owner, per-item-code holding count.
type InventoryCount struct {
	Key struct {
		OwnerUserID bson.ObjectID `bson:"ownerUserId"`
		ItemCode    string        `bson:"itemCode"`
	} `bson:"_id"`
	Count int `bson:"count"`
}

// AggregateActiveInventory counts non-destroyed equipment per owner and item
// code, excluding DISMANTLED/BROKEN and empty placeholders.
func (s *ItemStore) AggregateActiveInventory(ctx context.Context) ([]InventoryCount, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "itemCode", Value: bson.D{{Key: "$ne", Value: ""}}},
			{Key: "status", Value: bson.D{{Key: "$nin", Value: bson.A{"DISMANTLED", "BROKEN"}}}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "ownerUserId", Value: "$ownerUserId"},
				{Key: "itemCode", Value: "$itemCode"},
			}},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []InventoryCount
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetActive returns MUs that are not disbanded (zero disbandedAt).
func (s *MuStore) GetActive(ctx context.Context) ([]Mu, error) {
	filter := bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "disbandedAt", Value: bson.D{{Key: "$exists", Value: false}}}},
		bson.D{{Key: "disbandedAt", Value: time.Time{}}},
	}}}
	cursor, err := s.coll.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Mu
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetActive returns parties that are not disbanded (zero disbandedAt).
func (s *PartyStore) GetActive(ctx context.Context) ([]Party, error) {
	filter := bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "disbandedAt", Value: bson.D{{Key: "$exists", Value: false}}}},
		bson.D{{Key: "disbandedAt", Value: time.Time{}}},
	}}}
	cursor, err := s.coll.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Party
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// OpenOffer is a live (not cancelled, not fully filled) order at a price level.
type OpenOffer struct {
	Price     float64 `bson:"price"`
	Remaining int     `bson:"remaining"`
}

// GetOpenOffers returns live remaining quantity per price for one item side,
// sorted by price (descending for BUY/bids, ascending for SELL/asks).
func (s *TradeOfferStore) GetOpenOffers(ctx context.Context, itemCode, side string, descending bool) ([]OpenOffer, error) {
	order := 1
	if descending {
		order = -1
	}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "itemCode", Value: itemCode},
			{Key: "side", Value: side},
			{Key: "cancelled", Value: false},
		}}},
		{{Key: "$addFields", Value: bson.D{
			{Key: "remaining", Value: bson.D{{Key: "$subtract", Value: bson.A{"$quantity", "$fulfilled"}}}},
		}}},
		{{Key: "$match", Value: bson.D{{Key: "remaining", Value: bson.D{{Key: "$gt", Value: 0}}}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$price"},
			{Key: "remaining", Value: bson.D{{Key: "$sum", Value: "$remaining"}}},
		}}},
		{{Key: "$project", Value: bson.D{
			{Key: "price", Value: "$_id"},
			{Key: "remaining", Value: 1},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "price", Value: order}}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []OpenOffer
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
