package reports

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// AllianceMoneyFlowCounterpart is one per-counterpart-alliance breakdown row.
type AllianceMoneyFlowCounterpart struct {
	AllianceID   bson.ObjectID `bson:"allianceId"`
	InEquipment  float64       `bson:"inEquipment"`
	OutEquipment float64       `bson:"outEquipment"`
	InItems      float64       `bson:"inItems"`
	OutItems     float64       `bson:"outItems"`
	InWages      float64       `bson:"inWages"`
	OutWages     float64       `bson:"outWages"`
}

// AllianceMoneyFlowReport is a per-alliance daily money-flow roll-up.
type AllianceMoneyFlowReport struct {
	ID         string        `bson:"_id"`
	AllianceID bson.ObjectID `bson:"allianceId"`
	DayStart   time.Time     `bson:"dayStart"`

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

	Counterparts []AllianceMoneyFlowCounterpart `bson:"counterparts"`
}

// AllianceMoneyFlowReportID is the deterministic per-alliance-per-day key.
func AllianceMoneyFlowReportID(allianceID bson.ObjectID, dayStart time.Time) string {
	return allianceID.Hex() + "@" + dayStart.UTC().Format("2006-01-02")
}

// AllianceMoneyFlowReportStore manages the alliance_money_flow_reports collection.
type AllianceMoneyFlowReportStore struct {
	coll *mongo.Collection
}

// NewAllianceMoneyFlowReportStore creates the store and ensures indexes.
func NewAllianceMoneyFlowReportStore(ctx context.Context, db *mongo.Database) *AllianceMoneyFlowReportStore {
	store := &AllianceMoneyFlowReportStore{coll: db.Collection("alliance_money_flow_reports")}
	store.ensureIndex(ctx)
	return store
}

func (s *AllianceMoneyFlowReportStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "allianceId", Value: 1}, {Key: "dayStart", Value: -1}},
	})
	if err != nil {
		slog.Error("Failed creating compound index on alliance_money_flow_reports.{allianceId,dayStart}", "error", err)
	}
}

// Upsert replaces a batch of per-alliance daily money-flow reports keyed on _id.
func (s *AllianceMoneyFlowReportStore) Upsert(ctx context.Context, rows []AllianceMoneyFlowReport) error {
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
