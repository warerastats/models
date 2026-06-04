package trackers

import (
	"context"
	"errors"
	"log/slog"

	"github.com/warerastats/models/models/enums"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
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
}

func (s *ItemStore) Exists(ctx context.Context, id bson.ObjectID) (bool, error) {
	count, err := s.coll.CountDocuments(ctx, bson.M{"_id": id})
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// Callers should treat mongo.ErrNoDocuments as a normal "missing" signal, not a hard error.
func (s *ItemStore) Get(ctx context.Context, id bson.ObjectID) (*Item, error) {
	var item Item
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&item)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (s *ItemStore) Create(
	ctx context.Context,
	id bson.ObjectID,
	itemCode string,
	skills map[string]float64,
	ownerUserID bson.ObjectID,
) error {
	_, err := s.coll.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "itemCode", Value: itemCode},
			{Key: "skills", Value: skills},
			{Key: "state", Value: 100},
			{Key: "status", Value: enums.PERFECT},
			{Key: "ownerUserId", Value: ownerUserID},
		}}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

func (s *ItemStore) CreateEmpty(ctx context.Context, ids []bson.ObjectID) error {
	if len(ids) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(ids))
	for _, id := range ids {
		models = append(models,
			mongo.NewUpdateOneModel().
				SetFilter(bson.D{{Key: "_id", Value: id}}).
				SetUpdate(bson.D{{Key: "$setOnInsert", Value: bson.D{
					{Key: "itemCode", Value: ""},
				}}}).
				SetUpsert(true),
		)
	}
	_, err := s.coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		var bwe mongo.BulkWriteException
		if errors.As(err, &bwe) {
			for _, we := range bwe.WriteErrors {
				if we.Code != 11000 {
					return err
				}
			}
			return nil
		}
		return err
	}
	return nil
}

func (s *ItemStore) UpsertEquipment(
	ctx context.Context,
	id bson.ObjectID,
	itemCode string,
	skills map[string]float64,
	state int,
	ownerUserID bson.ObjectID,
) error {
	status := enums.PERFECT
	if state != 100 {
		status = enums.USED
	}
	_, err := s.coll.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "itemCode", Value: itemCode},
			{Key: "skills", Value: skills},
			{Key: "state", Value: state},
			{Key: "status", Value: status},
			{Key: "ownerUserId", Value: ownerUserID},
		}}},
		options.UpdateOne().SetUpsert(true),
	)
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
