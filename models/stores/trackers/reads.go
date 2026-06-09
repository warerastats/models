package trackers

import (
	"context"
	"time"

	"github.com/warerastats/models/models/enums"
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

// CountByCountry counts battles each country participated in as attacker or defender.
func (s *BattleStore) CountByCountry(ctx context.Context) ([]CountryAgg, error) {
	return s.countByCountry(ctx, nil)
}

// CountByCountryFinalized counts finalized (winner decided) battles per country.
func (s *BattleStore) CountByCountryFinalized(ctx context.Context) ([]CountryAgg, error) {
	return s.countByCountry(ctx, bson.D{{Key: "winnerSide", Value: bson.D{{Key: "$ne", Value: nil}}}})
}

// countByCountry counts battles per participating country, optionally filtered.
func (s *BattleStore) countByCountry(ctx context.Context, match bson.D) ([]CountryAgg, error) {
	pipeline := mongo.Pipeline{}
	if len(match) > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: match}})
	}
	pipeline = append(pipeline,
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "countries", Value: bson.A{"$attackerCountryId", "$defenderCountryId"}},
		}}},
		bson.D{{Key: "$unwind", Value: "$countries"}},
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$countries"},
			{Key: "memberCount", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	)
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

// GetFinalized returns the ids of all battles whose winner side has been set.
func (s *BattleStore) GetFinalized(ctx context.Context) ([]bson.ObjectID, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "winnerSide", Value: bson.D{{Key: "$ne", Value: nil}}}},
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

// UserAttrs is a lightweight projection of a user's country and MU.
type UserAttrs struct {
	ID        bson.ObjectID  `bson:"_id"`
	CountryID bson.ObjectID  `bson:"countryId"`
	MuID      *bson.ObjectID `bson:"muId,omitempty"`
}

// GetManyAttrs returns only id, countryId, and muId for the given user ids.
func (s *UserStore) GetManyAttrs(ctx context.Context, ids []bson.ObjectID) ([]UserAttrs, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		options.Find().SetProjection(bson.D{
			{Key: "_id", Value: 1},
			{Key: "countryId", Value: 1},
			{Key: "muId", Value: 1},
		}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []UserAttrs
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

// withCursor appends an exclusive _id < before clause to a base filter for
// newest-first keyset pagination. before may be nil for the first page.
func withCursor(base bson.D, before *bson.ObjectID) bson.D {
	out := make(bson.D, len(base), len(base)+1)
	copy(out, base)
	if before != nil {
		out = append(out, bson.E{Key: "_id", Value: bson.D{{Key: "$lt", Value: *before}}})
	}
	return out
}

// pageLimit clamps a requested page size into a sane range.
func pageLimit(limit int) int64 {
	if limit <= 0 {
		return 20
	}
	if limit > 200 {
		return 200
	}
	return int64(limit)
}

// newestFirst is a Find option set sorting by _id descending with a limit.
func newestFirst(limit int) *options.FindOptionsBuilder {
	return options.Find().
		SetSort(bson.D{{Key: "_id", Value: -1}}).
		SetLimit(pageLimit(limit))
}

// GetByCountryPaged returns users in a country, newest first, keyset-paginated.
func (s *UserStore) GetByCountryPaged(ctx context.Context, countryID bson.ObjectID, before *bson.ObjectID, limit int) ([]User, error) {
	filter := withCursor(bson.D{{Key: "countryId", Value: countryID}}, before)
	cursor, err := s.coll.Find(ctx, filter, newestFirst(limit))
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

// GetByRulingParty returns the countries currently ruled by a party.
func (s *CountryStore) GetByRulingParty(ctx context.Context, partyID bson.ObjectID) ([]Country, error) {
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "rulingPartyId", Value: partyID}})
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

// GetByCountry returns all regions belonging to a country.
func (s *RegionStore) GetByCountry(ctx context.Context, countryID bson.ObjectID) ([]Region, error) {
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "countryId", Value: countryID}})
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

// GetMany returns region documents for the given ids.
func (s *RegionStore) GetMany(ctx context.Context, ids []bson.ObjectID) ([]Region, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
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

// activeFilter matches non-disbanded entities (missing or zero disbandedAt).
func activeFilter() bson.D {
	return bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "disbandedAt", Value: bson.D{{Key: "$exists", Value: false}}}},
		bson.D{{Key: "disbandedAt", Value: time.Time{}}},
	}}}
}

// GetByCountryPaged returns parties in a country, newest first, keyset-paginated.
func (s *PartyStore) GetByCountryPaged(ctx context.Context, countryID bson.ObjectID, before *bson.ObjectID, limit int) ([]Party, error) {
	filter := withCursor(bson.D{{Key: "countryId", Value: countryID}}, before)
	cursor, err := s.coll.Find(ctx, filter, newestFirst(limit))
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

// GetByRegion returns parties headquartered in a region.
func (s *PartyStore) GetByRegion(ctx context.Context, regionID bson.ObjectID) ([]Party, error) {
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "regionId", Value: regionID}})
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

// ListActive returns parties, newest first, keyset-paginated. When activeOnly
// is set, disbanded parties are excluded.
func (s *PartyStore) ListActive(ctx context.Context, activeOnly bool, before *bson.ObjectID, limit int) ([]Party, error) {
	base := bson.D{}
	if activeOnly {
		base = activeFilter()
	}
	cursor, err := s.coll.Find(ctx, withCursor(base, before), newestFirst(limit))
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

// GetMany returns party documents for the given ids.
func (s *PartyStore) GetMany(ctx context.Context, ids []bson.ObjectID) ([]Party, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
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

// GetByRegion returns mus headquartered in a region.
func (s *MuStore) GetByRegion(ctx context.Context, regionID bson.ObjectID) ([]Mu, error) {
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "regionId", Value: regionID}})
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

// ListActive returns mus, newest first, keyset-paginated. When activeOnly is
// set, disbanded mus are excluded.
func (s *MuStore) ListActive(ctx context.Context, activeOnly bool, before *bson.ObjectID, limit int) ([]Mu, error) {
	base := bson.D{}
	if activeOnly {
		base = activeFilter()
	}
	cursor, err := s.coll.Find(ctx, withCursor(base, before), newestFirst(limit))
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

// GetMany returns mu documents for the given ids.
func (s *MuStore) GetMany(ctx context.Context, ids []bson.ObjectID) ([]Mu, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
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

// BattleFilter selects which battles a listing returns.
type BattleFilter int

const (
	BattleFilterAll BattleFilter = iota
	BattleFilterActive
	BattleFilterFinalized
)

// List returns battles newest first, keyset-paginated, optionally filtered by
// activity/finalization state.
func (s *BattleStore) List(ctx context.Context, filter BattleFilter, before *bson.ObjectID, limit int) ([]Battle, error) {
	base := applyBattleFilter(bson.D{}, filter)
	cursor, err := s.coll.Find(ctx, withCursor(base, before), newestFirst(limit))
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

// applyBattleFilter narrows a battle query base by active/finalized state.
func applyBattleFilter(base bson.D, filter BattleFilter) bson.D {
	switch filter {
	case BattleFilterActive:
		return append(base, bson.E{Key: "active", Value: true})
	case BattleFilterFinalized:
		return append(base, bson.E{Key: "winnerSide", Value: bson.D{{Key: "$ne", Value: nil}}})
	default:
		return base
	}
}

// GetByCountryPaged returns battles a country fought in (attacker or defender),
// newest first, keyset-paginated, narrowed by filter.
func (s *BattleStore) GetByCountryPaged(ctx context.Context, countryID bson.ObjectID, filter BattleFilter, before *bson.ObjectID, limit int) ([]Battle, error) {
	base := applyBattleFilter(bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "attackerCountryId", Value: countryID}},
		bson.D{{Key: "defenderCountryId", Value: countryID}},
	}}}, filter)
	cursor, err := s.coll.Find(ctx, withCursor(base, before), newestFirst(limit))
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

// GetByRegionPaged returns battles fought in a region (attacker or defender),
// newest first, keyset-paginated, narrowed by filter.
func (s *BattleStore) GetByRegionPaged(ctx context.Context, regionID bson.ObjectID, filter BattleFilter, before *bson.ObjectID, limit int) ([]Battle, error) {
	base := applyBattleFilter(bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "attackerRegionId", Value: regionID}},
		bson.D{{Key: "defenderRegionId", Value: regionID}},
	}}}, filter)
	cursor, err := s.coll.Find(ctx, withCursor(base, before), newestFirst(limit))
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

// GetByBattlePaged returns damage rows for a battle, newest first,
// keyset-paginated, optionally narrowed to a side and/or user.
func (s *DamageStore) GetByBattlePaged(ctx context.Context, battleID bson.ObjectID, before *bson.ObjectID, limit int, side *enums.Side, userID *bson.ObjectID) ([]Damage, error) {
	base := bson.D{{Key: "battleId", Value: battleID}}
	if side != nil {
		base = append(base, bson.E{Key: "side", Value: *side})
	}
	if userID != nil {
		base = append(base, bson.E{Key: "userId", Value: *userID})
	}
	cursor, err := s.coll.Find(ctx, withCursor(base, before), newestFirst(limit))
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

// GetByUserPaged returns a user's damage rows across all battles, newest first.
func (s *DamageStore) GetByUserPaged(ctx context.Context, userID bson.ObjectID, before *bson.ObjectID, limit int) ([]Damage, error) {
	filter := withCursor(bson.D{{Key: "userId", Value: userID}}, before)
	cursor, err := s.coll.Find(ctx, filter, newestFirst(limit))
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

// GetBattleIDsByPartyPaged returns distinct battle ids a party dealt damage in,
// newest first, keyset-paginated by battle id.
func (s *DamageStore) GetBattleIDsByPartyPaged(ctx context.Context, partyID bson.ObjectID, before *bson.ObjectID, limit int) ([]bson.ObjectID, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "partyId", Value: partyID}}}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$battleId"}}}},
	}
	if before != nil {
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: bson.D{
			{Key: "_id", Value: bson.D{{Key: "$lt", Value: *before}}},
		}}})
	}
	pipeline = append(pipeline,
		bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: -1}}}},
		bson.D{{Key: "$limit", Value: pageLimit(limit)}},
	)
	cursor, err := s.coll.Aggregate(ctx, pipeline)
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

// GetBattleIDsByUserPaged returns distinct battle ids a user dealt damage in,
// newest first, keyset-paginated by battle id.
func (s *DamageStore) GetBattleIDsByUserPaged(ctx context.Context, userID bson.ObjectID, before *bson.ObjectID, limit int) ([]bson.ObjectID, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "userId", Value: userID}}}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$battleId"}}}},
	}
	if before != nil {
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: bson.D{
			{Key: "_id", Value: bson.D{{Key: "$lt", Value: *before}}},
		}}})
	}
	pipeline = append(pipeline,
		bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: -1}}}},
		bson.D{{Key: "$limit", Value: pageLimit(limit)}},
	)
	cursor, err := s.coll.Aggregate(ctx, pipeline)
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

// PartyParticipationAgg is a party's cumulative battle damage rollup.
type PartyParticipationAgg struct {
	TotalDamage int64 `bson:"totalDamage"`
	BattleCount int   `bson:"battleCount"`
}

// AggregatePartyParticipation rolls up a party's total damage and distinct
// battle count across all damage rows.
func (s *DamageStore) AggregatePartyParticipation(ctx context.Context, partyID bson.ObjectID) (PartyParticipationAgg, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "partyId", Value: partyID}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
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
		return PartyParticipationAgg{}, err
	}
	defer cursor.Close(ctx)

	var rows []PartyParticipationAgg
	err = cursor.All(ctx, &rows)
	if err != nil {
		return PartyParticipationAgg{}, err
	}
	if len(rows) == 0 {
		return PartyParticipationAgg{}, nil
	}
	return rows[0], nil
}

// AggregateBattleDamage ranks users by total damage within a single battle.
func (s *DamageStore) AggregateBattleDamage(ctx context.Context, battleID bson.ObjectID, limit int) ([]UserDamageAgg, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "battleId", Value: battleID}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$userId"},
			{Key: "totalDamage", Value: bson.D{{Key: "$sum", Value: "$damages"}}},
		}}},
		{{Key: "$project", Value: bson.D{
			{Key: "totalDamage", Value: 1},
			{Key: "battleCount", Value: bson.D{{Key: "$literal", Value: 1}}},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "totalDamage", Value: -1}}}},
		{{Key: "$limit", Value: pageLimit(limit)}},
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

// GetByOwnerPaged returns items owned by a user, newest first, keyset-paginated,
// optionally narrowed to a status.
func (s *ItemStore) GetByOwnerPaged(ctx context.Context, userID bson.ObjectID, before *bson.ObjectID, limit int, status *enums.ItemStatus) ([]Item, error) {
	base := bson.D{{Key: "ownerUserId", Value: userID}}
	if status != nil {
		base = append(base, bson.E{Key: "status", Value: *status})
	}
	cursor, err := s.coll.Find(ctx, withCursor(base, before), newestFirst(limit))
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

// GetHistoryForUser returns a user's skill snapshots, newest first, paginated.
func (s *SkillStore) GetHistoryForUser(ctx context.Context, userID bson.ObjectID, before *bson.ObjectID, limit int) ([]Skill, error) {
	filter := withCursor(bson.D{{Key: "userId", Value: userID}}, before)
	cursor, err := s.coll.Find(ctx, filter, newestFirst(limit))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Skill
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetByUser returns the companies owned by a user.
func (s *CompanyStore) GetByUser(ctx context.Context, userID bson.ObjectID) ([]Company, error) {
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "userId", Value: userID}})
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

// GetByRegionPaged returns companies located in a region, newest first.
func (s *CompanyStore) GetByRegionPaged(ctx context.Context, regionID bson.ObjectID, before *bson.ObjectID, limit int) ([]Company, error) {
	filter := withCursor(bson.D{{Key: "regionId", Value: regionID}}, before)
	cursor, err := s.coll.Find(ctx, filter, newestFirst(limit))
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

// GetMany returns company documents for the given ids.
func (s *CompanyStore) GetMany(ctx context.Context, ids []bson.ObjectID) ([]Company, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
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

// Get returns a single trade offer by id.
func (s *TradeOfferStore) Get(ctx context.Context, id bson.ObjectID) (*TradeOffer, error) {
	var offer TradeOffer
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&offer)
	if err != nil {
		return nil, err
	}
	return &offer, nil
}

// GetByUserPaged returns a user's trade offers, newest first, keyset-paginated,
// optionally narrowed to an item code and/or side.
func (s *TradeOfferStore) GetByUserPaged(ctx context.Context, userID bson.ObjectID, before *bson.ObjectID, limit int, itemCode *string, side *enums.TradeSide) ([]TradeOffer, error) {
	base := bson.D{{Key: "userId", Value: userID}}
	if itemCode != nil {
		base = append(base, bson.E{Key: "itemCode", Value: *itemCode})
	}
	if side != nil {
		base = append(base, bson.E{Key: "side", Value: *side})
	}
	cursor, err := s.coll.Find(ctx, withCursor(base, before), newestFirst(limit))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []TradeOffer
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
