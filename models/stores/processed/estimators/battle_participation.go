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
