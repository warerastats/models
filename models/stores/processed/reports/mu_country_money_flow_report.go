package reports

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MuCountryMoneyFlowCounterpart is one per-country breakdown row for an MU.
type MuCountryMoneyFlowCounterpart struct {
	CountryID    bson.ObjectID `bson:"countryId"`
	InEquipment  float64       `bson:"inEquipment"`
	OutEquipment float64       `bson:"outEquipment"`
	InItems      float64       `bson:"inItems"`
	OutItems     float64       `bson:"outItems"`
	InWages      float64       `bson:"inWages"`
	OutWages     float64       `bson:"outWages"`
}

// MuCountryMoneyFlowReport is a per-MU daily money-flow roll-up.
type MuCountryMoneyFlowReport struct {
	ID       string        `bson:"_id"`
	MuID     bson.ObjectID `bson:"muId"`
	DayStart time.Time     `bson:"dayStart"`

	InEquipment  float64 `bson:"inEquipment"`
	OutEquipment float64 `bson:"outEquipment"`
	InItems      float64 `bson:"inItems"`
	OutItems     float64 `bson:"outItems"`
	InWages      float64 `bson:"inWages"`
	OutWages     float64 `bson:"outWages"`

	InEquipmentInsideMu  float64 `bson:"inEquipmentInsideMu"`
	OutEquipmentInsideMu float64 `bson:"outEquipmentInsideMu"`
	InItemsInsideMu      float64 `bson:"inItemsInsideMu"`
	OutItemsInsideMu     float64 `bson:"outItemsInsideMu"`
	InWagesInsideMu      float64 `bson:"inWagesInsideMu"`
	OutWagesInsideMu     float64 `bson:"outWagesInsideMu"`

	InEquipmentSameCountryOutsideMu  float64 `bson:"inEquipmentSameCountryOutsideMu"`
	OutEquipmentSameCountryOutsideMu float64 `bson:"outEquipmentSameCountryOutsideMu"`
	InItemsSameCountryOutsideMu      float64 `bson:"inItemsSameCountryOutsideMu"`
	OutItemsSameCountryOutsideMu     float64 `bson:"outItemsSameCountryOutsideMu"`
	InWagesSameCountryOutsideMu      float64 `bson:"inWagesSameCountryOutsideMu"`
	OutWagesSameCountryOutsideMu     float64 `bson:"outWagesSameCountryOutsideMu"`

	InEquipmentCrossBorderOutsideMuCountry  float64 `bson:"inEquipmentCrossBorderOutsideMuCountry"`
	OutEquipmentCrossBorderOutsideMuCountry float64 `bson:"outEquipmentCrossBorderOutsideMuCountry"`
	InItemsCrossBorderOutsideMuCountry      float64 `bson:"inItemsCrossBorderOutsideMuCountry"`
	OutItemsCrossBorderOutsideMuCountry     float64 `bson:"outItemsCrossBorderOutsideMuCountry"`
	InWagesCrossBorderOutsideMuCountry      float64 `bson:"inWagesCrossBorderOutsideMuCountry"`
	OutWagesCrossBorderOutsideMuCountry     float64 `bson:"outWagesCrossBorderOutsideMuCountry"`

	Counterparts []MuCountryMoneyFlowCounterpart `bson:"counterparts"`
}

// MuCountryMoneyFlowReportID is the deterministic per-mu-per-day key.
func MuCountryMoneyFlowReportID(muID bson.ObjectID, dayStart time.Time) string {
	return muID.Hex() + "@" + dayStart.UTC().Format("2006-01-02")
}

type MuCountryMoneyFlowReportStore struct {
	coll *mongo.Collection
}

func NewMuCountryMoneyFlowReportStore(ctx context.Context, db *mongo.Database) *MuCountryMoneyFlowReportStore {
	store := &MuCountryMoneyFlowReportStore{coll: db.Collection("mu_country_money_flow_reports")}
	store.ensureIndex(ctx)
	return store
}

func (s *MuCountryMoneyFlowReportStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "muId", Value: 1}, {Key: "dayStart", Value: -1}},
	})
	if err != nil {
		slog.Error("Failed creating compound index on mu_country_money_flow_reports.{muId,dayStart}", "error", err)
	}
}

// Upsert replaces a batch of per-MU daily money-flow reports keyed on _id.
func (s *MuCountryMoneyFlowReportStore) Upsert(ctx context.Context, rows []MuCountryMoneyFlowReport) error {
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
