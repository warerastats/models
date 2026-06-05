package models

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/warerastats/models/models/stores/events"
	"github.com/warerastats/models/models/stores/states"
	"github.com/warerastats/models/models/stores/trackers"
	"github.com/warerastats/models/models/stores/transactions"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type Collections struct {
	client *mongo.Client

	Transactions TransactionsCollection
	Trackers     TrackersCollection
	States       StatesCollection
	Events       EventsCollection
}

type EventsCollection struct {
	UserMUChange      *events.UserMUChangeStore
	UserNameChange    *events.UserNameChangeStore
	UserPartyChange   *events.UserPartyChangeStore
	UserCompanyChange *events.UserCompanyChangeStore
	UserCountryChange *events.UserCountryChangeStore
	UserSkillChange   *events.UserSkillChangeStore

	RegionOwnerChange       *events.RegionOwnerChangeStore
	RegionDeposit           *events.RegionDepositStore
	RegionStrategicResource *events.RegionStrategicResourceStore
	BattleOrderChange       *events.BattleOrderChangeStore

	CountryRulingPartyChange    *events.CountryRulingPartyChangeStore
	CountrySpecialisationChange *events.CountrySpecialisationChangeStore

	CompanyRegionChange   *events.CompanyRegionChangeStore
	CompanyItemCodeChange *events.CompanyItemCodeChangeStore
	EmployeeWageChange    *events.EmployeeWageChangeStore
}

type StatesCollection struct {
	ScraperState *states.ScraperStateStore
}

type TransactionsCollection struct {
	CaseTransaction      *transactions.CaseTransactionStore
	CraftTransaction     *transactions.CraftTransactionStore
	DismantleTransaction *transactions.DismantleTransactionStore
	LootTransaction      *transactions.LootTransactionStore
	MarketTransaction    *transactions.MarketTransactionStore
	TradeTransaction     *transactions.TradeTransactionStore
	WageTransaction      *transactions.WageTransactionStore
}

type TrackersCollection struct {
	Item      *trackers.ItemStore
	User      *trackers.UserStore
	Country   *trackers.CountryStore
	Region    *trackers.RegionStore
	Party     *trackers.PartyStore
	Mu        *trackers.MuStore
	Battle    *trackers.BattleStore
	Damage    *trackers.DamageStore
	Skill     *trackers.SkillStore
	Company   *trackers.CompanyStore
	Employee  *trackers.EmployeeStore
	ItemOffer *trackers.ItemOfferStore
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

		Transactions: TransactionsCollection{
			CaseTransaction:      transactions.NewCaseTransactionStore(ctx, db),
			CraftTransaction:     transactions.NewCraftTransactionStore(ctx, db),
			DismantleTransaction: transactions.NewDismantleTransactionStore(ctx, db),
			LootTransaction:      transactions.NewLootTransactionStore(ctx, db),
			MarketTransaction:    transactions.NewMarketTransactionStore(ctx, db),
			TradeTransaction:     transactions.NewTradeTransactionStore(ctx, db),
			WageTransaction:      transactions.NewWageTransactionStore(ctx, db),
		},

		Trackers: TrackersCollection{
			Item:      trackers.NewItemStore(ctx, db),
			User:      trackers.NewUserStore(ctx, db),
			Country:   trackers.NewCountryStore(ctx, db),
			Region:    trackers.NewRegionStore(ctx, db),
			Party:     trackers.NewPartyStore(ctx, db),
			Mu:        trackers.NewMuStore(ctx, db),
			Battle:    trackers.NewBattleStore(ctx, db),
			Damage:    trackers.NewDamageStore(ctx, db),
			Skill:     trackers.NewSkillStore(ctx, db),
			Company:   trackers.NewCompanyStore(ctx, db),
			Employee:  trackers.NewEmployeeStore(ctx, db),
			ItemOffer: trackers.NewItemOfferStore(ctx, db),
		},

		States: StatesCollection{
			ScraperState: states.NewScraperStateStore(ctx, db),
		},

		Events: EventsCollection{
			UserMUChange:      events.NewUserMUChangeStore(ctx, db),
			UserNameChange:    events.NewUserNameChangeStore(ctx, db),
			UserPartyChange:   events.NewUserPartyChangeStore(ctx, db),
			UserCompanyChange: events.NewUserCompanyChangeStore(ctx, db),
			UserCountryChange: events.NewUserCountryChangeStore(ctx, db),
			UserSkillChange:   events.NewUserSkillChangeStore(ctx, db),

			RegionOwnerChange:       events.NewRegionOwnerChangeStore(ctx, db),
			RegionDeposit:           events.NewRegionDepositStore(ctx, db),
			RegionStrategicResource: events.NewRegionStrategicResourceStore(ctx, db),
			BattleOrderChange:       events.NewBattleOrderChangeStore(ctx, db),

			CountryRulingPartyChange:    events.NewCountryRulingPartyChangeStore(ctx, db),
			CountrySpecialisationChange: events.NewCountrySpecialisationChangeStore(ctx, db),

			CompanyRegionChange:   events.NewCompanyRegionChangeStore(ctx, db),
			CompanyItemCodeChange: events.NewCompanyItemCodeChangeStore(ctx, db),
			EmployeeWageChange:    events.NewEmployeeWageChangeStore(ctx, db),
		},
	}
}

func (c Collections) Close(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_ = c.client.Disconnect(ctx)
}
