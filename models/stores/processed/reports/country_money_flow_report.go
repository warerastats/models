package reports

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// CountryMoneyFlowCounterpart is one per-counterpart-country breakdown row.
type CountryMoneyFlowCounterpart struct {
	CountryID    bson.ObjectID `bson:"countryId"`
	InEquipment  float64       `bson:"inEquipment"`
	OutEquipment float64       `bson:"outEquipment"`
	InItems      float64       `bson:"inItems"`
	OutItems     float64       `bson:"outItems"`
	InWages      float64       `bson:"inWages"`
	OutWages     float64       `bson:"outWages"`
}

// CountryMoneyFlowReport is a per-country daily money-flow roll-up.
type CountryMoneyFlowReport struct {
	ID        string        `bson:"_id"`
	CountryID bson.ObjectID `bson:"countryId"`
	DayStart  time.Time     `bson:"dayStart"`

	InEquipment  float64 `bson:"inEquipment"`
	OutEquipment float64 `bson:"outEquipment"`
	InItems      float64 `bson:"inItems"`
	OutItems     float64 `bson:"outItems"`
	InWages      float64 `bson:"inWages"`
	OutWages     float64 `bson:"outWages"`

	InEquipmentDomestic     float64 `bson:"inEquipmentDomestic"`
	OutEquipmentDomestic    float64 `bson:"outEquipmentDomestic"`
	InItemsDomestic         float64 `bson:"inItemsDomestic"`
	OutItemsDomestic        float64 `bson:"outItemsDomestic"`
	InWagesDomestic         float64 `bson:"inWagesDomestic"`
	OutWagesDomestic        float64 `bson:"outWagesDomestic"`
	InEquipmentCrossBorder  float64 `bson:"inEquipmentCrossBorder"`
	OutEquipmentCrossBorder float64 `bson:"outEquipmentCrossBorder"`
	InItemsCrossBorder      float64 `bson:"inItemsCrossBorder"`
	OutItemsCrossBorder     float64 `bson:"outItemsCrossBorder"`
	InWagesCrossBorder      float64 `bson:"inWagesCrossBorder"`
	OutWagesCrossBorder     float64 `bson:"outWagesCrossBorder"`

	Counterparts []CountryMoneyFlowCounterpart `bson:"counterparts"`
}

// CountryMoneyFlowReportID is the deterministic per-country-per-day key.
func CountryMoneyFlowReportID(countryID bson.ObjectID, dayStart time.Time) string {
	return countryID.Hex() + "@" + dayStart.UTC().Format("2006-01-02")
}

type CountryMoneyFlowReportStore struct {
	coll *mongo.Collection
}

func NewCountryMoneyFlowReportStore(ctx context.Context, db *mongo.Database) *CountryMoneyFlowReportStore {
	store := &CountryMoneyFlowReportStore{coll: db.Collection("country_money_flow_reports")}
	store.ensureIndex(ctx)
	return store
}

func (s *CountryMoneyFlowReportStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "countryId", Value: 1}, {Key: "dayStart", Value: -1}},
	})
	if err != nil {
		slog.Error("Failed creating compound index on country_money_flow_reports.{countryId,dayStart}", "error", err)
	}
}

// Upsert replaces a batch of per-country daily money-flow reports keyed on _id.
func (s *CountryMoneyFlowReportStore) Upsert(ctx context.Context, rows []CountryMoneyFlowReport) error {
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
