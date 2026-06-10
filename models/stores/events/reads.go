package events

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MuBattleCount is the number of distinct battles a MU set an order in.
type MuBattleCount struct {
	MuID    bson.ObjectID `bson:"_id"`
	Battles int           `bson:"battles"`
}

// CountByMu returns, per MU, the count of distinct battles it added a MU order in.
func (s *BattleOrderChangeStore) CountByMu(ctx context.Context) ([]MuBattleCount, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "kind", Value: "mu"},
			{Key: "action", Value: "added"},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "muId", Value: "$entityId"},
				{Key: "battleId", Value: "$battleId"},
			}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$_id.muId"},
			{Key: "battles", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []MuBattleCount
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CountByMuFinalized returns, per MU, the count of distinct finalized battles it
// added a MU order in (joined to the battles collection by winnerSide).
func (s *BattleOrderChangeStore) CountByMuFinalized(ctx context.Context) ([]MuBattleCount, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "kind", Value: "mu"},
			{Key: "action", Value: "added"},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "muId", Value: "$entityId"},
				{Key: "battleId", Value: "$battleId"},
			}},
		}}},
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "battles"},
			{Key: "localField", Value: "_id.battleId"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "battle"},
		}}},
		{{Key: "$unwind", Value: "$battle"}},
		{{Key: "$match", Value: bson.D{
			{Key: "battle.winnerSide", Value: bson.D{{Key: "$ne", Value: nil}}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$_id.muId"},
			{Key: "battles", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []MuBattleCount
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BattleMuOrder pairs a battle with a MU that set an order in it.
type BattleMuOrder struct {
	BattleID bson.ObjectID `bson:"battleId"`
	MuID     bson.ObjectID `bson:"muId"`
}

// MuOrdersByBattles returns the distinct (battle, MU) order pairs for the given battles.
func (s *BattleOrderChangeStore) MuOrdersByBattles(ctx context.Context, battleIDs []bson.ObjectID) ([]BattleMuOrder, error) {
	if len(battleIDs) == 0 {
		return nil, nil
	}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "kind", Value: "mu"},
			{Key: "action", Value: "added"},
			{Key: "battleId", Value: bson.D{{Key: "$in", Value: battleIDs}}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "battleId", Value: "$battleId"},
				{Key: "muId", Value: "$entityId"},
			}},
		}}},
		{{Key: "$project", Value: bson.D{
			{Key: "battleId", Value: "$_id.battleId"},
			{Key: "muId", Value: "$_id.muId"},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []BattleMuOrder
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// listByField returns newest-first, keyset-paginated change events whose hang-off
// field matches id. before may be nil for the first page; limit is clamped.
func listByField[T any](ctx context.Context, coll *mongo.Collection, field string, id bson.ObjectID, before *bson.ObjectID, limit int) ([]T, error) {
	filter := bson.D{{Key: field, Value: id}}
	if before != nil {
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$lt", Value: *before}}})
	}
	l := int64(20)
	switch {
	case limit > 200:
		l = 200
	case limit > 0:
		l = int64(limit)
	}
	cursor, err := coll.Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}).SetLimit(l),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []T
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// User-scoped history lists.

func (s *UserNameChangeStore) ListByUser(ctx context.Context, userID bson.ObjectID, before *bson.ObjectID, limit int) ([]UserNameChange, error) {
	return listByField[UserNameChange](ctx, s.coll, "userId", userID, before, limit)
}

func (s *UserCountryChangeStore) ListByUser(ctx context.Context, userID bson.ObjectID, before *bson.ObjectID, limit int) ([]UserCountryChange, error) {
	return listByField[UserCountryChange](ctx, s.coll, "userId", userID, before, limit)
}

func (s *UserPartyChangeStore) ListByUser(ctx context.Context, userID bson.ObjectID, before *bson.ObjectID, limit int) ([]UserPartyChange, error) {
	return listByField[UserPartyChange](ctx, s.coll, "userId", userID, before, limit)
}

func (s *UserCompanyChangeStore) ListByUser(ctx context.Context, userID bson.ObjectID, before *bson.ObjectID, limit int) ([]UserCompanyChange, error) {
	return listByField[UserCompanyChange](ctx, s.coll, "userId", userID, before, limit)
}

func (s *UserMUChangeStore) ListByUser(ctx context.Context, userID bson.ObjectID, before *bson.ObjectID, limit int) ([]UserMUChange, error) {
	return listByField[UserMUChange](ctx, s.coll, "userId", userID, before, limit)
}

func (s *UserSkillChangeStore) ListByUser(ctx context.Context, userID bson.ObjectID, before *bson.ObjectID, limit int) ([]UserSkillChange, error) {
	return listByField[UserSkillChange](ctx, s.coll, "userId", userID, before, limit)
}

func (s *EmployeeWageChangeStore) ListByUser(ctx context.Context, userID bson.ObjectID, before *bson.ObjectID, limit int) ([]EmployeeWageChange, error) {
	return listByField[EmployeeWageChange](ctx, s.coll, "userId", userID, before, limit)
}

// Party-scoped history lists.

func (s *PartyNameChangeStore) ListByParty(ctx context.Context, partyID bson.ObjectID, before *bson.ObjectID, limit int) ([]PartyNameChange, error) {
	return listByField[PartyNameChange](ctx, s.coll, "partyId", partyID, before, limit)
}

func (s *PartyLeaderChangeStore) ListByParty(ctx context.Context, partyID bson.ObjectID, before *bson.ObjectID, limit int) ([]PartyLeaderChange, error) {
	return listByField[PartyLeaderChange](ctx, s.coll, "partyId", partyID, before, limit)
}

func (s *PartyDescriptionChangeStore) ListByParty(ctx context.Context, partyID bson.ObjectID, before *bson.ObjectID, limit int) ([]PartyDescriptionChange, error) {
	return listByField[PartyDescriptionChange](ctx, s.coll, "partyId", partyID, before, limit)
}

func (s *PartyEthicsChangeStore) ListByParty(ctx context.Context, partyID bson.ObjectID, before *bson.ObjectID, limit int) ([]PartyEthicsChange, error) {
	return listByField[PartyEthicsChange](ctx, s.coll, "partyId", partyID, before, limit)
}

// Country-scoped history lists.

func (s *CountryRulingPartyChangeStore) ListByCountry(ctx context.Context, countryID bson.ObjectID, before *bson.ObjectID, limit int) ([]CountryRulingPartyChange, error) {
	return listByField[CountryRulingPartyChange](ctx, s.coll, "countryId", countryID, before, limit)
}

func (s *CountrySpecialisationChangeStore) ListByCountry(ctx context.Context, countryID bson.ObjectID, before *bson.ObjectID, limit int) ([]CountrySpecialisationChange, error) {
	return listByField[CountrySpecialisationChange](ctx, s.coll, "countryId", countryID, before, limit)
}

func (s *CountryAllianceJoinStore) ListByCountry(ctx context.Context, countryID bson.ObjectID, before *bson.ObjectID, limit int) ([]CountryAllianceJoin, error) {
	return listByField[CountryAllianceJoin](ctx, s.coll, "countryId", countryID, before, limit)
}

func (s *CountryAllianceLeaveStore) ListByCountry(ctx context.Context, countryID bson.ObjectID, before *bson.ObjectID, limit int) ([]CountryAllianceLeave, error) {
	return listByField[CountryAllianceLeave](ctx, s.coll, "countryId", countryID, before, limit)
}

// Region-scoped history lists.

func (s *RegionOwnerChangeStore) ListByRegion(ctx context.Context, regionID bson.ObjectID, before *bson.ObjectID, limit int) ([]RegionOwnerChange, error) {
	return listByField[RegionOwnerChange](ctx, s.coll, "regionId", regionID, before, limit)
}

func (s *RegionDepositStore) ListByRegion(ctx context.Context, regionID bson.ObjectID, before *bson.ObjectID, limit int) ([]RegionDeposit, error) {
	return listByField[RegionDeposit](ctx, s.coll, "regionId", regionID, before, limit)
}

func (s *RegionStrategicResourceStore) ListByRegion(ctx context.Context, regionID bson.ObjectID, before *bson.ObjectID, limit int) ([]RegionStrategicResource, error) {
	return listByField[RegionStrategicResource](ctx, s.coll, "regionId", regionID, before, limit)
}

// Company-scoped history lists.

func (s *CompanyRegionChangeStore) ListByCompany(ctx context.Context, companyID bson.ObjectID, before *bson.ObjectID, limit int) ([]CompanyRegionChange, error) {
	return listByField[CompanyRegionChange](ctx, s.coll, "companyId", companyID, before, limit)
}

func (s *CompanyItemCodeChangeStore) ListByCompany(ctx context.Context, companyID bson.ObjectID, before *bson.ObjectID, limit int) ([]CompanyItemCodeChange, error) {
	return listByField[CompanyItemCodeChange](ctx, s.coll, "companyId", companyID, before, limit)
}

// Mu-scoped history lists.

func (s *MuNameChangeStore) ListByMu(ctx context.Context, muID bson.ObjectID, before *bson.ObjectID, limit int) ([]MuNameChange, error) {
	return listByField[MuNameChange](ctx, s.coll, "muId", muID, before, limit)
}

func (s *MuOwnerChangeStore) ListByMu(ctx context.Context, muID bson.ObjectID, before *bson.ObjectID, limit int) ([]MuOwnerChange, error) {
	return listByField[MuOwnerChange](ctx, s.coll, "muId", muID, before, limit)
}

func (s *MuMercenaryReputationChangeStore) ListByMu(ctx context.Context, muID bson.ObjectID, before *bson.ObjectID, limit int) ([]MuMercenaryReputationChange, error) {
	return listByField[MuMercenaryReputationChange](ctx, s.coll, "muId", muID, before, limit)
}

// ListByBattle returns a battle's order changes, newest first, keyset-paginated.
func (s *BattleOrderChangeStore) ListByBattle(ctx context.Context, battleID bson.ObjectID, before *bson.ObjectID, limit int) ([]BattleOrderChange, error) {
	return listByField[BattleOrderChange](ctx, s.coll, "battleId", battleID, before, limit)
}

// ListByEntity returns order changes for a given entity kind and id, newest first, keyset-paginated.
func (s *BattleOrderChangeStore) ListByEntity(ctx context.Context, kind string, entityID bson.ObjectID, before *bson.ObjectID, limit int) ([]BattleOrderChange, error) {
	filter := bson.D{
		{Key: "kind", Value: kind},
		{Key: "entityId", Value: entityID},
	}
	if before != nil {
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$lt", Value: *before}}})
	}
	l := int64(20)
	switch {
	case limit > 200:
		l = 200
	case limit > 0:
		l = int64(limit)
	}
	cursor, err := s.coll.Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}).SetLimit(l),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []BattleOrderChange
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListBattleIDsByMu returns the distinct battle ids a MU set an order in, newest
// first, keyset-paginated on the battle id.
func (s *BattleOrderChangeStore) ListBattleIDsByMu(ctx context.Context, muID bson.ObjectID, before *bson.ObjectID, limit int) ([]bson.ObjectID, error) {
	match := bson.D{
		{Key: "kind", Value: "mu"},
		{Key: "action", Value: "added"},
		{Key: "entityId", Value: muID},
	}
	if before != nil {
		match = append(match, bson.E{Key: "battleId", Value: bson.D{{Key: "$lt", Value: *before}}})
	}
	l := 20
	switch {
	case limit > 200:
		l = 200
	case limit > 0:
		l = limit
	}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: match}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$battleId"}}}},
		{{Key: "$sort", Value: bson.D{{Key: "_id", Value: -1}}}},
		{{Key: "$limit", Value: l}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var rows []struct {
		ID bson.ObjectID `bson:"_id"`
	}
	err = cursor.All(ctx, &rows)
	if err != nil {
		return nil, err
	}
	out := make([]bson.ObjectID, len(rows))
	for i := range rows {
		out[i] = rows[i].ID
	}
	return out, nil
}
