package models

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/warerastats/models/models/stores/trackers"
	"github.com/warerastats/models/models/stores/unprocessed"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type Collections struct {
	client *mongo.Client

	Unprocessed UnprocessedCollection
	Trackers    TrackersCollection
}

type UnprocessedCollection struct {
	CaseTransaction *unprocessed.CaseTransactionStore
}

type TrackersCollection struct {
	Item    *trackers.ItemStore
	User    *trackers.UserStore
	Country *trackers.CountryStore
	Region  *trackers.RegionStore
}

func Init(context.Context) (*Collections, error) {
	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		slog.Error("No mongodb URI given!")
		os.Exit(1)
	}

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(uri).SetServerAPIOptions(serverAPI)

	var err error
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}

	database := client.Database("data")
	collections := newCollections(ctx, database)
	return collections, nil
}

func newCollections(ctx context.Context, db *mongo.Database) *Collections {
	return &Collections{
		client: db.Client(),

		Unprocessed: UnprocessedCollection{
			CaseTransaction: unprocessed.NewCaseTransactionStore(ctx, db),
		},

		Trackers: TrackersCollection{
			Item:    trackers.NewItemStore(ctx, db),
			User:    trackers.NewUserStore(ctx, db),
			Country: trackers.NewCountryStore(ctx, db),
			Region:  trackers.NewRegionStore(ctx, db),
		},
	}
}

func (c Collections) Close(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_ = c.client.Disconnect(ctx)
}
