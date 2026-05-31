package trackers

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Skill struct {
	ID     bson.ObjectID `bson:"id,omitempty"`
	UserID bson.ObjectID `bson:"userId"`
	Skills UserSkills    `bson:"skills"`
	Since  time.Time     `bson:"since"`
}

type SkillStore struct {
	coll *mongo.Collection
}

func NewSkillStore(ctx context.Context, db *mongo.Database) *SkillStore {
	store := &SkillStore{
		coll: db.Collection("skills"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *SkillStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "userId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on skills.userId",
			"error", err,
		)
		return
	}
}

type UserSkills struct {
	Energy           int `bson:"energy"`
	Health           int `bson:"health"`
	Hunger           int `bson:"hunger"`
	Attack           int `bson:"attack"`
	Companies        int `bson:"companies"`
	Entrepreneurship int `bson:"entrepreneurship"`
	Production       int `bson:"production"`
	CriticalChance   int `bson:"criticalChance"`
	CriticalDamages  int `bson:"criticalDamages"`
	Armor            int `bson:"armor"`
	Precision        int `bson:"precision"`
	Dodge            int `bson:"dodge"`
	LootChance       int `bson:"lootChance"`
	Management       int `bson:"management"`
}
