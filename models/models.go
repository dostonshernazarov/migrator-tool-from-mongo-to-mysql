package models

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// MySQL Models
type Service struct {
	ID        string    `gorm:"primaryKey;column:id;size:36;not null"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
	Name      string    `gorm:"column:name;size:255;not null"`
	Code      string    `gorm:"column:code;size:36;not null;uniqueIndex"`
}

func (Service) TableName() string { return "services" }

type Organization struct {
	ID                           string     `gorm:"primaryKey;column:id;size:36;not null"`
	CreatedAt                    time.Time  `gorm:"column:created_at;not null"`
	UpdatedAt                    time.Time  `gorm:"column:updated_at"`
	DeletedAt                    *time.Time `gorm:"column:deleted_at"`
	IsDeleted                    bool       `gorm:"column:is_deleted"`
	Name                         string     `gorm:"column:name; not null"`
	Inn                          *string    `gorm:"column:inn"`
	Pinfl                        *string    `gorm:"column:pinfl"`
	Balance                      float64    `gorm:"column:balance"`
	FiscalizationBalance         float64    `gorm:"column:fiscalization_balance"`
	ReservedFiscalizationBalance float64    `gorm:"column:reserved_fiscalization_balance"`
	TotalPayments                float64    `gorm:"column:total_payments"`
	CreditAmount                 float64    `gorm:"column:credit_amount"`
	OrganizationCode             string     `gorm:"column:organization_code"`
	ReferralAgentCode            *string    `gorm:"column:referral_agent_code"`
	WhiteLabel                   string     `gorm:"column:white-label"`
	OfferNumber                  string     `gorm:"column:offer_number"`
	OfferDate                    *time.Time `gorm:"column:offer_date"`
}

func (Organization) TableName() string { return "organizations" }

type OrganizationServiceDemoUses struct {
	OrganizationId string    `gorm:"column:organization_id;size:36;not null"`
	ServiceCode    string    `gorm:"column:service_code;size:36;not null"`
	UsedAt         time.Time `gorm:"column:used_at;"`
}

func (OrganizationServiceDemoUses) TableName() string { return "organization_service_demo_uses" }

type Package struct {
	ID                          string    `gorm:"primaryKey;column:id;size:36;not null"`
	CreatedAt                   time.Time `gorm:"column:created_at;not null"`
	IsDeleted                   bool      `gorm:"column:is_deleted"`
	Name                        string    `gorm:"column:name; not null"`
	Price                       float64   `gorm:"column:price"`
	BRVRate                     float64   `gorm:"column:brv_rate"`
	DurationDays                int       `gorm:"column:duration_days"`
	DurationMonths              int       `gorm:"column:duration_months"`
	IsDemo                      bool      `gorm:"column:is_demo"`
	IsPublic                    bool      `gorm:"column:is_public"`
	ServiceCode                 string    `gorm:"column:service_code;size:36"`
	DefaultSetOnNewOrganization bool      `gorm:"column:default_set_on_new_organization"`
}

func (Package) TableName() string { return "packages" }

type PackageItem struct {
	ID                 string  `gorm:"primaryKey;column:id;size:36;not null"`
	PackageId          string  `gorm:"column:package_id;size:36;not null"`
	Name               string  `gorm:"column:name;size:255;not null"`
	Code               int     `gorm:"column:code;not null"`
	IsOverLimitAllowed bool    `gorm:"column:is_over_limit_allowed"`
	OverLimitPrice     float64 `gorm:"column:over_limit_price"`
	BRVRate            float64 `gorm:"column:brv_rate"`
	IsUnlimited        bool    `gorm:"column:is_unlimited"`
	Limit              int     `gorm:"column:limit"`
}

func (PackageItem) TableName() string { return "package_items" }

type PackageActivationBonusPackage struct {
	PackageId      string `gorm:"column:package_id;size:36;not null"`
	BonusPackageId string `gorm:"column:bonus_package_id;size:36;not null"`
}

func (PackageActivationBonusPackage) TableName() string { return "package_activation_bonus_packages" }

type BoughtPackage struct {
	ID             string    `gorm:"primaryKey;column:id;size:36;not null"`
	OrganizationId string    `gorm:"column:organization_id;size:36"`
	PackageId      string    `gorm:"column:package_id;size:36"`
	BoughtAt       time.Time `gorm:"column:bought_at;not null"`
	ExpiresAt      time.Time `gorm:"column:expires_at;not null"`
	IsAutoExtend   bool      `gorm:"column:is_auto_extend"`
	IsActive       bool      `gorm:"column:is_active"`
	Price          float64   `gorm:"column:price;not null"`
}

func (BoughtPackage) TableName() string { return "bought_packages" }

type BoughtPackageItem struct {
	ID                 string  `gorm:"primaryKey;column:id;size:36;not null"`
	BoughtPackageId    string  `gorm:"column:bought_package_id;size:36"`
	Name               string  `gorm:"column:name;size:255;not null"`
	Code               int     `gorm:"column:code;not null"`
	IsOverLimitAllowed bool    `gorm:"column:is_over_limit_allowed"`
	OverLimitPrice     float64 `gorm:"column:over_limit_price"`
	IsUnlimited        bool    `gorm:"column:is_unlimited"`
	LimitValue         int     `gorm:"column:limit_value"`
	UsedCount          int     `gorm:"column:used_count"`
}

func (BoughtPackageItem) TableName() string { return "bought_package_items" }

type Charge struct {
	ID                    string     `gorm:"primaryKey;column:id;size:36;not null"`
	CreatedAt             time.Time  `gorm:"column:created_at;not null"`
	IsDeleted             bool       `gorm:"column:is_deleted"`
	OrganizationId        string     `gorm:"column:organization_id;size:36"`
	Price                 float64    `gorm:"column:price;not null"`
	Type                  int        `gorm:"column:type"`
	BoughtPackageID       string     `gorm:"column:bought_package_id;size:36;not null"`
	BoughtPackageItemCode int        `gorm:"column:bought_package_item_code;not null"`
	ServiceCode           string     `gorm:"column:service_code;size:36"`
	ObjectId              string     `gorm:"column:object_id;size:36"`
	Number                string     `gorm:"column:number;size:36"`
	Date1                 *time.Time `gorm:"column:date1"`
	Date2                 *time.Time `gorm:"column:date2"`
}

func (Charge) TableName() string { return "charges" }

type Payment struct {
	ID                string    `gorm:"primaryKey;column:id;size:36;not null"`
	CreatedAt         time.Time `gorm:"column:created_at;not null"`
	Amount            float64   `gorm:"column:amount;not null"`
	OrganizationID    string    `gorm:"column:organization_id;size:36;not null"`
	AccountID         string    `gorm:"column:account_id;size:36;not null"`
	Method            int       `gorm:"column:method;not null"`
	BankTransactionID *string   `gorm:"column:bank_transaction_id;size:36"`
}

func (Payment) TableName() string { return "payments" }

type PaymeTransaction struct {
	ID                 string     `gorm:"primaryKey;column:id;size:36;not null"`
	CreatedAt          time.Time  `gorm:"column:created_at;not null"`
	PaymeTransactionID string     `gorm:"column:payme_transaction_id;size:36;not null"`
	PaymeCreatedAt     time.Time  `gorm:"column:payme_created_at;not null"`
	SystemCompletedAt  *time.Time `gorm:"column:system_completed_at"`
	State              int        `gorm:"column:state"`
	Amount             float64    `gorm:"column:amount;not null"`
	PaymentId          *string    `gorm:"column:payment_id"`
	OrganizationID     string     `gorm:"column:organization_id;size:36;not null"`
	Reason             int        `gorm:"column:reason"`
	SystemCanceledAt   *time.Time `gorm:"column:system_canceled_at"`
}

func (PaymeTransaction) TableName() string { return "payme_transactions" }

type OrganizationBalanceBinding struct {
	ID                     string     `gorm:"primaryKey;column:id;size:36;not null"`
	CreatedAt              time.Time  `gorm:"column:created_at;not null"`
	DeletedAt              *time.Time `gorm:"column:deleted_at"`
	IsDeleted              bool       `gorm:"column:is_deleted"`
	PayerOrganizationID    string     `gorm:"column:payer_organization_id;size:36"`
	TargetOrganizationID   string     `gorm:"column:target_organization_id;size:36"`
	PayerOrganizationName  string     `gorm:"column:payer_organization_name"`
	TargetOrganizationName string     `gorm:"column:target_organization_name"`
}

func (OrganizationBalanceBinding) TableName() string { return "organization_balance_bindings" }

type CreditUpdates struct {
	ID             string    `gorm:"primaryKey;column:id;size:36;not null"`
	CreatedAt      time.Time `gorm:"column:created_at;not null"`
	OrganizationID string    `gorm:"column:organization_id;size:36;not null;index:idx_organization-id,priority:1"`
	Amount         float64   `gorm:"column:amount;not null"`
	AccountID      string    `gorm:"column:account_id;size:36"`
}

func (CreditUpdates) TableName() string { return "credit_updates" }

type BankPaymentAutoApplyError struct {
	ID            string    `gorm:"primaryKey;column:id;size:36"`
	CreatedAt     time.Time `gorm:"column:created_at;not null"`
	ErrorMessage  string    `gorm:"column:error_message;type:text"`
	Amount        float64   `gorm:"column:amount;not null"`
	TransactionID string    `gorm:"column:transaction_id;size:36;index:idx_transaction_id;not null"`
	PayerInn      string    `gorm:"column:payer_inn;size:14;not null"`
	PayerName     string    `gorm:"column:payer_name;size:255;not null"`
	Description   *string   `gorm:"column:description;type:text"`
	Resolved      bool      `gorm:"column:resolved;default:false"`
}

func (BankPaymentAutoApplyError) TableName() string { return "bank_payments_auto_apply_errors" }

// MongoDB Models (for decoding)
type MongoService struct {
	ID        primitive.ObjectID `bson:"_id"`
	CreatedAt time.Time          `bson:"created_at"`
	Name      string             `bson:"name"`
	Code      string             `bson:"code"`
}

type MongoOrganization struct {
	ID                           primitive.ObjectID `bson:"_id"`
	CreatedAt                    time.Time          `bson:"created_at"`
	UpdatedAt                    time.Time          `bson:"updated_at"`
	DeletedAt                    *time.Time         `bson:"deleted_at"`
	IsDeleted                    bool               `bson:"is_deleted"`
	Name                         string             `bson:"name"`
	Inn                          *string            `bson:"inn"`
	Pinfl                        *string            `bson:"pinfl"`
	Balance                      float64            `bson:"balance"`
	FiscalizationBalance         float64            `bson:"fiscalization_balance"`
	ReservedFiscalizationBalance float64            `bson:"reserved_fiscalization_balance"`
	TotalPayments                float64            `bson:"total_payments"`
	CreditAmount                 float64            `bson:"credit_amount"`
	OrganizationCode             string             `bson:"organization_code"`
	ReferralAgentCode            *string            `bson:"referral_agent_code"`
	WhiteLabel                   string             `bson:"white_label"`
	OfferInfo                    struct {
		Number string     `bson:"number"`
		Date   *time.Time `bson:"date"`
	} `bson:"offer_info"`
	ActivePackages []struct {
		ID           string       `bson:"_id, omitempty"`
		BoughtAt     time.Time    `bson:"bought_at"`
		ExpiresAt    time.Time    `bson:"expires_at"`
		IsAutoExtend bool         `bson:"is_auto_extend"`
		Package      MongoPackage `bson:"package"`
	} `bson:"active_packages"`
	ServiceDemoUses []struct {
		ID   primitive.ObjectID `bson:"_id"`
		Name string             `bson:"name"`
		Code string             `bson:"code"`
	} `bson:"service_demo_uses"`
}

type mongoPackageItem struct {
	Name               string  `bson:"name"`
	Code               int     `bson:"code"`
	IsOverLimitAllowed bool    `bson:"is_over_limit_allowed"`
	OverLimitPrice     float64 `bson:"over_limit_price"`
	BRVRate            float64 `bson:"brv_rate"`
	IsUnlimited        bool    `bson:"is_unlimited"`
	Limit              int     `bson:"limit"`
}

type MongoPackage struct {
	ID             primitive.ObjectID `bson:"_id"`
	CreatedAt      time.Time          `bson:"created_at"`
	UpdatedAt      time.Time          `bson:"updated_at"`
	DeletedAt      *time.Time         `bson:"deleted_at"`
	IsDeleted      bool               `bson:"is_deleted"`
	Name           string             `bson:"name"`
	Price          float64            `bson:"price"`
	BRVRate        float64            `bson:"brv_rate"`
	DurationDays   int                `bson:"duration_days"`
	DurationMonths int                `bson:"duration_months"`
	IsDemo         bool               `bson:"is_demo"`
	IsPublic       bool               `bson:"is_public"`
	Service        struct {
		ID   primitive.ObjectID `bson:"_id"`
		Name string             `bson:"name"`
		Code string             `bson:"code"`
	} `bson:"service"`
	Items                       []mongoPackageItem `bson:"items"`
	DefaultSetOnNewOrganization bool               `bson:"default_set_on_new_organization"`
	OnActivationBonusPackages   []struct {
		ID primitive.ObjectID `bson:"_id"`
	} `bson:"on_activation_bonus_packages"`
}

// Database interface
type Database interface {
	Migrate() error
	GetDB() *gorm.DB
}

type database struct {
	db *gorm.DB
}

func (d *database) GetDB() *gorm.DB {
	return d.db
}

func NewDatabase(username, password, addr, databaseName, timezone string) (Database, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=%s",
		username, password, addr, databaseName, timezone)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		SkipDefaultTransaction: true,
	})
	if err != nil {
		return nil, err
	}

	return &database{db: db}, nil
}

func (d *database) Migrate() error {
	// Drop and recreate tables to ensure schema is correct
	tables := []interface{}{
		&Service{},
		&Organization{},
		&OrganizationServiceDemoUses{},
		&Package{},
		&PackageItem{},
		&PackageActivationBonusPackage{},
		&BoughtPackage{},
		&BoughtPackageItem{},
		&Charge{},
		&Payment{},
		&PaymeTransaction{},
		&OrganizationBalanceBinding{},
		&CreditUpdates{},
		&BankPaymentAutoApplyError{},
	}

	for _, table := range tables {
		if err := d.db.Migrator().DropTable(table); err != nil {
			// Ignore errors if table doesn't exist
		}
	}

	return d.db.AutoMigrate(tables...)
}
