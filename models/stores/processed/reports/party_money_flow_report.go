package reports

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// PartyMoneyFlowCounterpart is one per-country breakdown row for a party.
type PartyMoneyFlowCounterpart struct {
	CountryID    bson.ObjectID `bson:"countryId"`
	InEquipment  float64       `bson:"inEquipment"`
	OutEquipment float64       `bson:"outEquipment"`
	InItems      float64       `bson:"inItems"`
	OutItems     float64       `bson:"outItems"`
	InWages      float64       `bson:"inWages"`
	OutWages     float64       `bson:"outWages"`
}

// PartyMoneyFlowReport is a per-party daily money-flow roll-up.
type PartyMoneyFlowReport struct {
	ID       string        `bson:"_id"`
	PartyID  bson.ObjectID `bson:"partyId"`
	DayStart time.Time     `bson:"dayStart"`

	InEquipment  float64 `bson:"inEquipment"`
	OutEquipment float64 `bson:"outEquipment"`
	InItems      float64 `bson:"inItems"`
	OutItems     float64 `bson:"outItems"`
	InWages      float64 `bson:"inWages"`
	OutWages     float64 `bson:"outWages"`

	InEquipmentInsideParty  float64 `bson:"inEquipmentInsideParty"`
	OutEquipmentInsideParty float64 `bson:"outEquipmentInsideParty"`
	InItemsInsideParty      float64 `bson:"inItemsInsideParty"`
	OutItemsInsideParty     float64 `bson:"outItemsInsideParty"`
	InWagesInsideParty      float64 `bson:"inWagesInsideParty"`
	OutWagesInsideParty     float64 `bson:"outWagesInsideParty"`

	InEquipmentSameCountryOutsideParty  float64 `bson:"inEquipmentSameCountryOutsideParty"`
	OutEquipmentSameCountryOutsideParty float64 `bson:"outEquipmentSameCountryOutsideParty"`
	InItemsSameCountryOutsideParty      float64 `bson:"inItemsSameCountryOutsideParty"`
	OutItemsSameCountryOutsideParty     float64 `bson:"outItemsSameCountryOutsideParty"`
	InWagesSameCountryOutsideParty      float64 `bson:"inWagesSameCountryOutsideParty"`
	OutWagesSameCountryOutsideParty     float64 `bson:"outWagesSameCountryOutsideParty"`

	InEquipmentSameAllianceCrossBorder  float64 `bson:"inEquipmentSameAllianceCrossBorder"`
	OutEquipmentSameAllianceCrossBorder float64 `bson:"outEquipmentSameAllianceCrossBorder"`
	InItemsSameAllianceCrossBorder      float64 `bson:"inItemsSameAllianceCrossBorder"`
	OutItemsSameAllianceCrossBorder     float64 `bson:"outItemsSameAllianceCrossBorder"`
	InWagesSameAllianceCrossBorder      float64 `bson:"inWagesSameAllianceCrossBorder"`
	OutWagesSameAllianceCrossBorder     float64 `bson:"outWagesSameAllianceCrossBorder"`

	InEquipmentOutsideAlliance  float64 `bson:"inEquipmentOutsideAlliance"`
	OutEquipmentOutsideAlliance float64 `bson:"outEquipmentOutsideAlliance"`
	InItemsOutsideAlliance      float64 `bson:"inItemsOutsideAlliance"`
	OutItemsOutsideAlliance     float64 `bson:"outItemsOutsideAlliance"`
	InWagesOutsideAlliance      float64 `bson:"inWagesOutsideAlliance"`
	OutWagesOutsideAlliance     float64 `bson:"outWagesOutsideAlliance"`

	Counterparts []PartyMoneyFlowCounterpart `bson:"counterparts"`
}

// PartyMoneyFlowReportID is the deterministic per-party-per-day key.
func PartyMoneyFlowReportID(partyID bson.ObjectID, dayStart time.Time) string {
	return partyID.Hex() + "@" + dayStart.UTC().Format("2006-01-02")
}

// PartyMoneyFlowReportStore manages the party_money_flow_reports collection.
type PartyMoneyFlowReportStore struct {
	coll *mongo.Collection
}

// NewPartyMoneyFlowReportStore creates the store and ensures indexes.
func NewPartyMoneyFlowReportStore(ctx context.Context, db *mongo.Database) *PartyMoneyFlowReportStore {
	store := &PartyMoneyFlowReportStore{coll: db.Collection("party_money_flow_reports")}
	store.ensureIndex(ctx)
	return store
}

func (s *PartyMoneyFlowReportStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "partyId", Value: 1}, {Key: "dayStart", Value: -1}},
	})
	if err != nil {
		slog.Error("Failed creating compound index on party_money_flow_reports.{partyId,dayStart}", "error", err)
	}
}

// Upsert replaces a batch of per-party daily money-flow reports keyed on _id.
func (s *PartyMoneyFlowReportStore) Upsert(ctx context.Context, rows []PartyMoneyFlowReport) error {
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
