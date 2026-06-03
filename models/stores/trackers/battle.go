package trackers

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Battle struct {
	ID                bson.ObjectID   `bson:"_id"`
	AttackerRegionID  *bson.ObjectID  `bson:"attackerRegionId,omitempty"`
	AttackerCountryID bson.ObjectID   `bson:"attackerCountryId"`
	AttackerDamages   int             `bson:"attackerDamages"`
	DefenderRegionID  bson.ObjectID   `bson:"defenderRegionId"`
	DefenderCountryID bson.ObjectID   `bson:"defenderCountryId"`
	DefenderDamages   int             `bson:"defenderDamages"`
	WinnerSide        *string         `bson:"winnerSide,omitempty"`
	IsActive          bool            `bson:"active"`
	LastUpdated       time.Time       `bson:"updated"`
	LatestObject      json.RawMessage `bson:"raw"`
}

type BattleStore struct {
	coll *mongo.Collection
}

func NewBattleStore(ctx context.Context, db *mongo.Database) *BattleStore {
	store := &BattleStore{
		coll: db.Collection("battles"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *BattleStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "_id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on battles._id",
			"error", err,
		)
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "active", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on battles.active",
			"error", err,
		)
	}
}

func (s *BattleStore) Get(ctx context.Context, id bson.ObjectID) (*Battle, error) {
	var battle Battle
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&battle)
	if err != nil {
		return nil, err
	}
	return &battle, nil
}

func (s *BattleStore) UpsertBattle(ctx context.Context, id bson.ObjectID, data Battle) error {
	data.ID = id
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		data,
		options.Replace().SetUpsert(true),
	)
	return err
}

func (s *BattleStore) GetActiveIDs(ctx context.Context) ([]bson.ObjectID, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "active", Value: true}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var ids []bson.ObjectID
	for cursor.Next(ctx) {
		var result struct {
			ID bson.ObjectID `bson:"_id"`
		}
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		ids = append(ids, result.ID)
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *BattleStore) MarkInactive(ctx context.Context, ids []bson.ObjectID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.coll.UpdateMany(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "active", Value: false},
			{Key: "updated", Value: time.Now().UTC()},
		}}},
	)
	return err
}
