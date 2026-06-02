package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type UserSkillChange struct {
	ID     bson.ObjectID  `bson:"_id,omitempty"`
	UserID bson.ObjectID  `bson:"userId"`
	Skills map[string]int `bson:"skills"`
}

type UserSkillChangeStore struct {
	coll *mongo.Collection
}

func NewUserSkillChangeStore(ctx context.Context, db *mongo.Database) *UserSkillChangeStore {
	store := &UserSkillChangeStore{
		coll: db.Collection("events_user_skills_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *UserSkillChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "userId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_user_skills_change.userId",
			"error", err,
		)
		return
	}
}

func (s *UserSkillChangeStore) Set(ctx context.Context, change UserSkillChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *UserSkillChangeStore) Get(ctx context.Context, userID bson.ObjectID) (*UserSkillChange, error) {
	var change UserSkillChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "userId", Value: userID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
