package reports

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// timeRange builds an inclusive [from, to] filter clause on the given field.
func timeRange(field string, from, to time.Time) bson.E {
	return bson.E{Key: field, Value: bson.D{
		{Key: "$gte", Value: from},
		{Key: "$lte", Value: to},
	}}
}

// optionalTimeRange builds a $gte/$lte bound for whichever of from/to is set,
// returning ok=false when neither bound is provided.
func optionalTimeRange(field string, from, to *time.Time) (bson.E, bool) {
	bounds := bson.D{}
	if from != nil {
		bounds = append(bounds, bson.E{Key: "$gte", Value: *from})
	}
	if to != nil {
		bounds = append(bounds, bson.E{Key: "$lte", Value: *to})
	}
	if len(bounds) == 0 {
		return bson.E{}, false
	}
	return bson.E{Key: field, Value: bounds}, true
}

// GetByCountryRange returns a country's hourly tax flows in [from, to], oldest first.
func (s *CountryTaxFlowStore) GetByCountryRange(ctx context.Context, countryID bson.ObjectID, from, to time.Time) ([]CountryTaxFlow, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "countryId", Value: countryID}, timeRange("hourStart", from, to)},
		options.Find().SetSort(bson.D{{Key: "hourStart", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []CountryTaxFlow
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetByUserRange returns a user's daily finance reports in [from, to], oldest first.
func (s *UserFinanceReportStore) GetByUserRange(ctx context.Context, userID bson.ObjectID, from, to time.Time) ([]UserFinanceReport, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "userId", Value: userID}, timeRange("dayStart", from, to)},
		options.Find().SetSort(bson.D{{Key: "dayStart", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []UserFinanceReport
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetByEntityRange returns an entity's daily wealth reports in [from, to], oldest first.
func (s *EntityWealthReportStore) GetByEntityRange(ctx context.Context, entityType string, entityID bson.ObjectID, from, to time.Time) ([]EntityWealthReport, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{
			{Key: "entityType", Value: entityType},
			{Key: "entityId", Value: entityID},
			timeRange("dayStart", from, to),
		},
		options.Find().SetSort(bson.D{{Key: "dayStart", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []EntityWealthReport
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetByCountryRange returns a country's daily money-flow reports in [from, to], oldest first.
func (s *CountryMoneyFlowReportStore) GetByCountryRange(ctx context.Context, countryID bson.ObjectID, from, to time.Time) ([]CountryMoneyFlowReport, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "countryId", Value: countryID}, timeRange("dayStart", from, to)},
		options.Find().SetSort(bson.D{{Key: "dayStart", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []CountryMoneyFlowReport
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetByMuRange returns an MU's daily money-flow reports in [from, to], oldest first.
func (s *MuCountryMoneyFlowReportStore) GetByMuRange(ctx context.Context, muID bson.ObjectID, from, to time.Time) ([]MuCountryMoneyFlowReport, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "muId", Value: muID}, timeRange("dayStart", from, to)},
		options.Find().SetSort(bson.D{{Key: "dayStart", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []MuCountryMoneyFlowReport
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetByPartyRange returns a party's daily money-flow reports in [from, to], oldest first.
func (s *PartyMoneyFlowReportStore) GetByPartyRange(ctx context.Context, partyID bson.ObjectID, from, to time.Time) ([]PartyMoneyFlowReport, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "partyId", Value: partyID}, timeRange("dayStart", from, to)},
		options.Find().SetSort(bson.D{{Key: "dayStart", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []PartyMoneyFlowReport
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetByBattle returns a battle's per-interval damage report rows, oldest first,
// optionally narrowed to [from, to] and/or an entity type and/or entity ids.
func (s *BattleDamageReportStore) GetByBattle(ctx context.Context, battleID bson.ObjectID, from, to *time.Time, entityType *string, entityIDs []bson.ObjectID) ([]BattleDamageReport, error) {
	filter := bson.D{{Key: "battleId", Value: battleID}}
	if r, ok := optionalTimeRange("intervalStart", from, to); ok {
		filter = append(filter, r)
	}
	if entityType != nil {
		filter = append(filter, bson.E{Key: "entityType", Value: *entityType})
	}
	if len(entityIDs) > 0 {
		filter = append(filter, bson.E{Key: "entityId", Value: bson.D{{Key: "$in", Value: entityIDs}}})
	}
	cursor, err := s.coll.Find(ctx,
		filter,
		options.Find().SetSort(bson.D{{Key: "intervalStart", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []BattleDamageReport
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SumDamageByEntity returns the total damage for one entity in [from, to).
func (s *BattleDamageReportStore) SumDamageByEntity(ctx context.Context, entityType string, entityID bson.ObjectID, from, to time.Time) (int64, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "entityType", Value: entityType},
			{Key: "entityId", Value: entityID},
			{Key: "intervalStart", Value: bson.D{
				{Key: "$gte", Value: from},
				{Key: "$lt", Value: to},
			}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$damage"}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, err
	}
	defer cursor.Close(ctx)

	var rows []struct {
		Total int64 `bson:"total"`
	}
	if err = cursor.All(ctx, &rows); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return rows[0].Total, nil
}

// BattleIDsWithReports returns the distinct battle ids that already have at least one damage report row.
func (s *BattleDamageReportStore) BattleIDsWithReports(ctx context.Context) ([]bson.ObjectID, error) {
	res := s.coll.Distinct(ctx, "battleId", bson.D{})
	if err := res.Err(); err != nil {
		return nil, err
	}
	var out []bson.ObjectID
	err := res.Decode(&out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BattleIDsMissingSideRows returns battle ids that have at least one report row but no row with entityType "side".
func (s *BattleDamageReportStore) BattleIDsMissingSideRows(ctx context.Context) ([]bson.ObjectID, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$battleId"},
			{Key: "hasSide", Value: bson.D{{Key: "$max", Value: bson.D{
				{Key: "$cond", Value: bson.A{
					bson.D{{Key: "$eq", Value: bson.A{"$entityType", "side"}}},
					true,
					false,
				}},
			}}}},
		}}},
		{{Key: "$match", Value: bson.D{{Key: "hasSide", Value: false}}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var rows []struct {
		ID bson.ObjectID `bson:"_id"`
	}
	if err = cursor.All(ctx, &rows); err != nil {
		return nil, err
	}
	out := make([]bson.ObjectID, len(rows))
	for i := range rows {
		out[i] = rows[i].ID
	}
	return out, nil
}

// GetRange returns market-state snapshots in [from, to], oldest first.
func (s *MarketStateStore) GetRange(ctx context.Context, from, to time.Time) ([]MarketState, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{timeRange("at", from, to)},
		options.Find().SetSort(bson.D{{Key: "at", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []MarketState
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetLatest returns the most recent market-state snapshot, or false when none.
func (s *MarketStateStore) GetLatest(ctx context.Context) (*MarketState, bool, error) {
	var st MarketState
	err := s.coll.FindOne(ctx, bson.D{},
		options.FindOne().SetSort(bson.D{{Key: "at", Value: -1}}),
	).Decode(&st)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &st, true, nil
}

// GetRange returns wage-market-state snapshots in [from, to], oldest first.
func (s *WageMarketStateStore) GetRange(ctx context.Context, from, to time.Time) ([]WageMarketState, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{timeRange("at", from, to)},
		options.Find().SetSort(bson.D{{Key: "at", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []WageMarketState
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetLatest returns the most recent wage-market-state snapshot, or false when none.
func (s *WageMarketStateStore) GetLatest(ctx context.Context) (*WageMarketState, bool, error) {
	var st WageMarketState
	err := s.coll.FindOne(ctx, bson.D{},
		options.FindOne().SetSort(bson.D{{Key: "at", Value: -1}}),
	).Decode(&st)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &st, true, nil
}

// GetRange returns hourly dismantle reports in [from, to], oldest first.
func (s *DismantleReportStore) GetRange(ctx context.Context, from, to time.Time) ([]DismantleReport, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{timeRange("hourStart", from, to)},
		options.Find().SetSort(bson.D{{Key: "hourStart", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []DismantleReport
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetAll returns every per-case report.
func (s *CasesReportStore) GetAll(ctx context.Context) ([]CasesReport, error) {
	cursor, err := s.coll.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []CasesReport
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Get returns the market report for an item code, or false when none exists.
func (s *ItemMarketReportStore) Get(ctx context.Context, itemCode string) (*ItemMarketReport, bool, error) {
	var r ItemMarketReport
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: itemCode}}).Decode(&r)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &r, true, nil
}

// GetWindow returns the per-window price summary for an item, or false when none.
func (s *EquipmentPricingStore) GetWindow(ctx context.Context, itemCode string, windowDays int) (*EquipmentWindowPrice, bool, error) {
	var r EquipmentWindowPrice
	err := s.windows.FindOne(ctx,
		bson.D{{Key: "_id", Value: EquipmentWindowPriceID(itemCode, windowDays)}},
	).Decode(&r)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &r, true, nil
}

// GetSkills returns the per-skill-combo price summaries for an item and window.
func (s *EquipmentPricingStore) GetSkills(ctx context.Context, itemCode string, windowDays int) ([]EquipmentSkillPrice, error) {
	cursor, err := s.skills.Find(ctx, bson.D{
		{Key: "itemCode", Value: itemCode},
		{Key: "windowDays", Value: windowDays},
	})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []EquipmentSkillPrice
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetByCountryRange returns a country's daily alliance money-flow reports in [from, to], oldest first.
func (s *CountryAllianceMoneyFlowReportStore) GetByCountryRange(ctx context.Context, countryID bson.ObjectID, from, to time.Time) ([]CountryAllianceMoneyFlowReport, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "countryId", Value: countryID}, timeRange("dayStart", from, to)},
		options.Find().SetSort(bson.D{{Key: "dayStart", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []CountryAllianceMoneyFlowReport
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetByAllianceRange returns an alliance's daily money-flow reports in [from, to], oldest first.
func (s *AllianceMoneyFlowReportStore) GetByAllianceRange(ctx context.Context, allianceID bson.ObjectID, from, to time.Time) ([]AllianceMoneyFlowReport, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "allianceId", Value: allianceID}, timeRange("dayStart", from, to)},
		options.Find().SetSort(bson.D{{Key: "dayStart", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []AllianceMoneyFlowReport
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetByMuRange returns an MU's daily alliance money-flow reports in [from, to], oldest first.
func (s *MuAllianceMoneyFlowReportStore) GetByMuRange(ctx context.Context, muID bson.ObjectID, from, to time.Time) ([]MuAllianceMoneyFlowReport, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "muId", Value: muID}, timeRange("dayStart", from, to)},
		options.Find().SetSort(bson.D{{Key: "dayStart", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []MuAllianceMoneyFlowReport
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
