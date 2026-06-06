package estimators

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// UserBattleParticipation holds a user's cumulative battle and equipment-cost counters.
type UserBattleParticipation struct {
	UserID                 bson.ObjectID      `bson:"_id"`
	TotalDamage            int64              `bson:"totalDamage"`
	BattlesParticipated    int                `bson:"battlesParticipated"`
	NegativeDamage         int64              `bson:"negativeDamage"`
	DismantledCount        map[string]float64 `bson:"dismantledCount"`
	DismantledValue        float64            `bson:"dismantledValue"`
	OwnCountryBattles      int                `bson:"ownCountryBattles"`
	OwnCountryParticipated int                `bson:"ownCountryParticipated"`
	MuOrderBattles         int                `bson:"muOrderBattles"`
	MuOrderParticipated    int                `bson:"muOrderParticipated"`
	UpdatedAt              time.Time          `bson:"updatedAt"`
}

type BattleParticipationStore struct {
	coll *mongo.Collection
}

func NewBattleParticipationStore(ctx context.Context, db *mongo.Database) *BattleParticipationStore {
	store := &BattleParticipationStore{coll: db.Collection("user_battle_participation")}
	store.ensureIndex(ctx)
	return store
}

func (s *BattleParticipationStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "updatedAt", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on user_battle_participation.updatedAt", "error", err)
	}
}

// Upsert replaces a user's participation counters keyed on _id.
func (s *BattleParticipationStore) Upsert(ctx context.Context, p UserBattleParticipation) error {
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: p.UserID}},
		p,
		options.Replace().SetUpsert(true),
	)
	return err
}

// IncrementDamage adds delta damage and negative-damage to a user's counters.
func (s *BattleParticipationStore) IncrementDamage(ctx context.Context, userID bson.ObjectID, totalDamage, negativeDamage int64) error {
	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: userID}},
		bson.D{
			{Key: "$inc", Value: bson.D{
				{Key: "totalDamage", Value: totalDamage},
				{Key: "negativeDamage", Value: negativeDamage},
			}},
			{Key: "$set", Value: bson.D{{Key: "updatedAt", Value: time.Now().UTC()}}},
		},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

// IncrementDismantle adds dismantled value and per-item-code counts to a user.
func (s *BattleParticipationStore) IncrementDismantle(ctx context.Context, userID bson.ObjectID, valueDelta float64, countDeltas map[string]float64) error {
	inc := bson.D{{Key: "dismantledValue", Value: valueDelta}}
	for code, delta := range countDeltas {
		inc = append(inc, bson.E{Key: "dismantledCount." + code, Value: delta})
	}
	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: userID}},
		bson.D{
			{Key: "$inc", Value: inc},
			{Key: "$set", Value: bson.D{{Key: "updatedAt", Value: time.Now().UTC()}}},
		},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

// IncrementBattleCounters adds distinct-battle participation counts and sets the
// current battle-count denominators for a user.
func (s *BattleParticipationStore) IncrementBattleCounters(ctx context.Context, userID bson.ObjectID, battles, ownCountry, muOrder, ownCountryBattles, muOrderBattles int) error {
	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: userID}},
		bson.D{
			{Key: "$inc", Value: bson.D{
				{Key: "battlesParticipated", Value: battles},
				{Key: "ownCountryParticipated", Value: ownCountry},
				{Key: "muOrderParticipated", Value: muOrder},
			}},
			{Key: "$set", Value: bson.D{
				{Key: "ownCountryBattles", Value: ownCountryBattles},
				{Key: "muOrderBattles", Value: muOrderBattles},
				{Key: "updatedAt", Value: time.Now().UTC()},
			}},
		},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

// Get returns a user's participation counters, or false when none exists.
func (s *BattleParticipationStore) Get(ctx context.Context, userID bson.ObjectID) (*UserBattleParticipation, bool, error) {
	var p UserBattleParticipation
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: userID}}).Decode(&p)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &p, true, nil
}

// SumDamageForUsers returns the total cumulative damage across the given users.
func (s *BattleParticipationStore) SumDamageForUsers(ctx context.Context, ids []bson.ObjectID) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$totalDamage"}}},
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
	err = cursor.All(ctx, &rows)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return rows[0].Total, nil
}
