package trackers

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Mu struct {
	ID                  bson.ObjectID `bson:"id"`
	OwnerUserID         bson.ObjectID `bson:"userId"`
	RegionID            bson.ObjectID `bson:"regionId"`
	Name                string        `bson:"name"`
	AvatarUrl           string        `bson:"avatarUrl"`
	HeadQuarterLevel    int           `bson:"hq"`
	DormitoriesLevel    int           `bson:"dorms"`
	MercenaryReputation float64       `bson:"mercRep"`
}

type MuStore struct {
	coll *mongo.Collection
}

func NewMuStore(ctx context.Context, db *mongo.Database) *MuStore {
	store := &MuStore{
		coll: db.Collection("mus"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *MuStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on mus.id",
			"error", err,
		)
		return
	}
}
