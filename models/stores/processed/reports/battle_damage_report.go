package reports

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// EquipmentUsage is destroyed/used equipment in a battle interval and its value.
type EquipmentUsage struct {
	ItemCode string  `bson:"itemCode"`
	Count    float64 `bson:"count"`
	Value    float64 `bson:"value"`
}

// BattleDamageReport is a per-battle, per-2-minute-interval, per-side, per-entity damage row.
type BattleDamageReport struct {
	ID            string           `bson:"_id"`
	BattleID      bson.ObjectID    `bson:"battleId"`
	IntervalStart time.Time        `bson:"intervalStart"`
	Side          string           `bson:"side"`
	EntityType    string           `bson:"entityType"`
	EntityID      bson.ObjectID    `bson:"entityId"`
	Damage        int64            `bson:"damage"`
	DamagePct     float64          `bson:"damagePct"`
	Equipment     []EquipmentUsage `bson:"equipment"`
}

// BattleDamageReportID is the deterministic per-battle-interval-side-entity key.
func BattleDamageReportID(battleID bson.ObjectID, intervalStart time.Time, side, entityType string, entityID bson.ObjectID) string {
	return battleID.Hex() + "@" + intervalStart.UTC().Format(time.RFC3339) + "@" + side + "@" + entityType + "@" + entityID.Hex()
}

type BattleDamageReportStore struct {
	coll *mongo.Collection
}

func NewBattleDamageReportStore(ctx context.Context, db *mongo.Database) *BattleDamageReportStore {
	store := &BattleDamageReportStore{coll: db.Collection("battle_damage_reports")}
	store.ensureIndex(ctx)
	return store
}

func (s *BattleDamageReportStore) ensureIndex(ctx context.Context) {
	idxs := []mongo.IndexModel{
		{Keys: bson.D{{Key: "battleId", Value: 1}, {Key: "intervalStart", Value: 1}}},
		{Keys: bson.D{{Key: "entityType", Value: 1}, {Key: "entityId", Value: 1}, {Key: "intervalStart", Value: 1}}},
	}
	_, err := s.coll.Indexes().CreateMany(ctx, idxs)
	if err != nil {
		slog.Error("Failed creating indexes on battle_damage_reports", "error", err)
	}
}

// Upsert replaces a batch of battle damage report rows keyed on _id.
func (s *BattleDamageReportStore) Upsert(ctx context.Context, rows []BattleDamageReport) error {
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
