package reports

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// CountryAllianceMoneyFlowCounterpart is one per-counterpart-alliance breakdown row.
type CountryAllianceMoneyFlowCounterpart struct {
	AllianceID   bson.ObjectID `bson:"allianceId"`
	InEquipment  float64       `bson:"inEquipment"`
	OutEquipment float64       `bson:"outEquipment"`
	InItems      float64       `bson:"inItems"`
	OutItems     float64       `bson:"outItems"`
	InWages      float64       `bson:"inWages"`
	OutWages     float64       `bson:"outWages"`
}

// CountryAllianceMoneyFlowReport is a per-country daily money-flow roll-up grouped by alliance.
type CountryAllianceMoneyFlowReport struct {
	ID        string        `bson:"_id"`
	CountryID bson.ObjectID `bson:"countryId"`
	DayStart  time.Time     `bson:"dayStart"`

	InEquipment  float64 `bson:"inEquipment"`
	OutEquipment float64 `bson:"outEquipment"`
	InItems      float64 `bson:"inItems"`
	OutItems     float64 `bson:"outItems"`
	InWages      float64 `bson:"inWages"`
	OutWages     float64 `bson:"outWages"`

	InEquipmentInAlliance  float64 `bson:"inEquipmentInAlliance"`
	OutEquipmentInAlliance float64 `bson:"outEquipmentInAlliance"`
	InItemsInAlliance      float64 `bson:"inItemsInAlliance"`
	OutItemsInAlliance     float64 `bson:"outItemsInAlliance"`
	InWagesInAlliance      float64 `bson:"inWagesInAlliance"`
	OutWagesInAlliance     float64 `bson:"outWagesInAlliance"`

	InEquipmentOutsideAlliance  float64 `bson:"inEquipmentOutsideAlliance"`
	OutEquipmentOutsideAlliance float64 `bson:"outEquipmentOutsideAlliance"`
	InItemsOutsideAlliance      float64 `bson:"inItemsOutsideAlliance"`
	OutItemsOutsideAlliance     float64 `bson:"outItemsOutsideAlliance"`
	InWagesOutsideAlliance      float64 `bson:"inWagesOutsideAlliance"`
	OutWagesOutsideAlliance     float64 `bson:"outWagesOutsideAlliance"`

	Counterparts []CountryAllianceMoneyFlowCounterpart `bson:"counterparts"`
}

// CountryAllianceMoneyFlowReportID is the deterministic per-country-per-day key.
func CountryAllianceMoneyFlowReportID(countryID bson.ObjectID, dayStart time.Time) string {
	return countryID.Hex() + "@" + dayStart.UTC().Format("2006-01-02")
}

// CountryAllianceMoneyFlowReportStore manages the country_alliance_money_flow_reports collection.
type CountryAllianceMoneyFlowReportStore struct {
	coll *mongo.Collection
}

// NewCountryAllianceMoneyFlowReportStore creates the store and ensures indexes.
func NewCountryAllianceMoneyFlowReportStore(ctx context.Context, db *mongo.Database) *CountryAllianceMoneyFlowReportStore {
	store := &CountryAllianceMoneyFlowReportStore{coll: db.Collection("country_alliance_money_flow_reports")}
	store.ensureIndex(ctx)
	return store
}

func (s *CountryAllianceMoneyFlowReportStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "countryId", Value: 1}, {Key: "dayStart", Value: -1}},
	})
	if err != nil {
		slog.Error("Failed creating compound index on country_alliance_money_flow_reports.{countryId,dayStart}", "error", err)
	}
}

// Upsert replaces a batch of per-country daily alliance money-flow reports keyed on _id.
func (s *CountryAllianceMoneyFlowReportStore) Upsert(ctx context.Context, rows []CountryAllianceMoneyFlowReport) error {
	if len(rows) == 0 {
		return nil
	}
	ops := make([]mongo.WriteModel, len(rows))
	for i := range rows {
		ops[i] = mongo.NewReplaceOneModel().
			SetFilter(bson.D{{Key: "_id", Value: rows[i].ID}}).
			SetReplacement(rows[i]).
			SetUpsert(true)
	}
	_, err := s.coll.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	return err
}
