package reports

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// EquipmentWindowPrice is a per-item, per-window weighted price summary.
type EquipmentWindowPrice struct {
	ID          string    `bson:"_id"`
	ItemCode    string    `bson:"itemCode"`
	WindowDays  int       `bson:"windowDays"`
	WeightedAvg float64   `bson:"weightedAvg"`
	Volume      int       `bson:"volume"`
	Count       int       `bson:"count"`
	UpdatedAt   time.Time `bson:"updatedAt"`
}

// EquipmentWindowPriceID is the deterministic per-item-per-window key.
func EquipmentWindowPriceID(itemCode string, windowDays int) string {
	return itemCode + "@" + itoa(windowDays)
}

// EquipmentSkillPrice is a per-item, per-window, per-skill-combo price summary.
type EquipmentSkillPrice struct {
	ID         string             `bson:"_id"`
	ItemCode   string             `bson:"itemCode"`
	WindowDays int                `bson:"windowDays"`
	SkillKey   string             `bson:"skillKey"`
	Skills     map[string]float64 `bson:"skills"`
	Min        float64            `bson:"min"`
	Max        float64            `bson:"max"`
	Avg        float64            `bson:"avg"`
	Volume     int                `bson:"volume"`
	UpdatedAt  time.Time          `bson:"updatedAt"`
}

// EquipmentSkillPriceID is the deterministic per-item-window-skill key.
func EquipmentSkillPriceID(itemCode string, windowDays int, skillKey string) string {
	return itemCode + "@" + itoa(windowDays) + "@" + skillKey
}

type EquipmentPricingStore struct {
	windows *mongo.Collection
	skills  *mongo.Collection
}

func NewEquipmentPricingStore(ctx context.Context, db *mongo.Database) *EquipmentPricingStore {
	store := &EquipmentPricingStore{
		windows: db.Collection("equipment_window_prices"),
		skills:  db.Collection("equipment_skill_prices"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *EquipmentPricingStore) ensureIndex(ctx context.Context) {
	_, err := s.windows.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "itemCode", Value: 1}, {Key: "windowDays", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating compound index on equipment_window_prices.{itemCode,windowDays}", "error", err)
	}

	_, err = s.skills.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "itemCode", Value: 1}, {Key: "windowDays", Value: 1}, {Key: "skillKey", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating compound index on equipment_skill_prices.{itemCode,windowDays,skillKey}", "error", err)
	}
}

// UpsertWindows replaces a batch of per-window prices keyed on _id.
func (s *EquipmentPricingStore) UpsertWindows(ctx context.Context, rows []EquipmentWindowPrice) error {
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
	_, err := s.windows.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	return err
}

// UpsertSkills replaces a batch of per-skill-combo prices keyed on _id.
func (s *EquipmentPricingStore) UpsertSkills(ctx context.Context, rows []EquipmentSkillPrice) error {
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
	_, err := s.skills.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	return err
}
