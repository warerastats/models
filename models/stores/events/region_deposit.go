package events

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type RegionDeposit struct {
	ID           bson.ObjectID `bson:"_id,omitempty"`
	RegionID     bson.ObjectID `bson:"regionId"`
	Type         *string       `bson:"type,omitempty"`
	StartsAt     *time.Time    `bson:"startsAt,omitempty"`
	EndsAt       *time.Time    `bson:"endsAt,omitempty"`
	BonusPercent *float64      `bson:"bonusPercent,omitempty"`
}

type RegionDepositStore struct {
	coll *mongo.Collection
}

func NewRegionDepositStore(ctx context.Context, db *mongo.Database) *RegionDepositStore {
	store := &RegionDepositStore{
		coll: db.Collection("events_region_deposit"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *RegionDepositStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "regionId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_region_deposit.regionId",
			"error", err,
		)
		return
	}
}

func (s *RegionDepositStore) Set(ctx context.Context, change RegionDeposit) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *RegionDepositStore) Get(ctx context.Context, regionID bson.ObjectID) (*RegionDeposit, error) {
	var change RegionDeposit
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "regionId", Value: regionID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
