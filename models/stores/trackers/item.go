package trackers

import (
	"context"
	"log/slog"

	"github.com/warerastats/models/models/enums"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Item struct {
	ID          bson.ObjectID      `bson:"_id,omitempty"`
	ItemCode    string             `bson:"itemCode"`
	Skills      map[string]float64 `bson:"skills"`
	State       int                `bson:"state"`
	Status      enums.ItemStatus   `bson:"status"`
	OwnerUserID bson.ObjectID      `bson:"ownerUserId"`
}

type ItemStore struct {
	coll *mongo.Collection
}

func NewItemStore(ctx context.Context, db *mongo.Database) *ItemStore {
	store := &ItemStore{
		coll: db.Collection("items"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *ItemStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "itemCode", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on items.itemCode",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "ownerUserId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on items.ownerUserId",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "_id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on items._id",
			"error", err,
		)
		return
	}
}

func (s *ItemStore) Exists(ctx context.Context, id bson.ObjectID) (bool, error) {
	count, err := s.coll.CountDocuments(ctx, bson.M{"_id": id})
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *ItemStore) Create(
	ctx context.Context,
	id bson.ObjectID,
	itemCode string,
	skills map[string]float64,
	ownerUserID bson.ObjectID) error {

	item := Item{
		ID:          id,
		ItemCode:    itemCode,
		Skills:      skills,
		State:       100,
		Status:      enums.PERFECT,
		OwnerUserID: ownerUserID,
	}
	_, err := s.coll.InsertOne(ctx, item)
	return err
}

func (s *ItemStore) SetOwnerUserID(ctx context.Context, id bson.ObjectID, ownerUserID bson.ObjectID) error {
	_, err := s.coll.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "ownerUserId", Value: ownerUserID}}}},
	)
	return err
}

func (s *ItemStore) SetStatus(ctx context.Context, id bson.ObjectID, status enums.ItemStatus) error {
	_, err := s.coll.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: status}}}},
	)
	return err
}
