package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gorm.io/gorm/clause"
)

func main() {
	// Flags
	mongoURI := flag.String("mongo-uri", getEnv("mongodb://localhost:27017", "mongodb://localhost:27017"), "MongoDB connection string")
	mongoDBName := flag.String("mongo-db", getEnv("billingService", "billingService"), "MongoDB database name")
	mysqlUser := flag.String("mysql-user", getEnv("root", "root"), "MySQL username")
	mysqlPass := flag.String("mysql-pass", getEnv("123", "123"), "MySQL password")
	mysqlAddr := flag.String("mysql-addr", getEnv("127.0.0.1:3306", "127.0.0.1:3306"), "MySQL address host:port")
	mysqlDBName := flag.String("mysql-db", getEnv("billingService", "billing_service"), "MySQL database name")
	tz := flag.String("tz", getEnv("TZ", "UTC"), "IANA timezone, e.g. UTC or Asia/Tashkent")
	flag.Parse()

	// Validate required parameters
	if *mongoURI == "" {
		log.Fatal("MongoDB URI is required")
	}
	if *mysqlPass == "" {
		log.Fatal("MySQL password is required")
	}

	log.Printf("Starting migration from MongoDB (%s/%s) to MySQL (%s@%s/%s)",
		*mongoURI, *mongoDBName, *mysqlUser, *mysqlAddr, *mysqlDBName)

	// Connect to MongoDB
	mongoClient, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(*mongoURI))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer func() {
		if err = mongoClient.Disconnect(context.TODO()); err != nil {
			log.Printf("Error disconnecting from MongoDB: %v", err)
		}
	}()

	mdb := mongoClient.Database(*mongoDBName)

	// Connect to MySQL
	mysql, err := NewDatabase(*mysqlUser, *mysqlPass, *mysqlAddr, *mysqlDBName, *tz)
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}

	// Run migrations
	if err := mysql.Migrate(); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}

	// Migrate data
	ctx := context.Background()
	if err := migrateAll(ctx, mdb, mysql); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	log.Println("Migration completed successfully!")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func migrateAll(ctx context.Context, mdb *mongo.Database, mysql Database) error {
	// Migrate in dependency order
	migrations := []struct {
		name string
		fn   func(context.Context, *mongo.Database, Database) error
	}{
		{"services", migrateServices},
		{"organizations", migrateOrganizations},
		{"packages", migratePackages},
		{"bought-packages", migrateBoughtPackages},
		{"charges", migrateCharges},
		{"payments", migratePayments},
		{"payme-transactions", migratePaymeTransactions},
		{"organization-balance-bindings", migrateOrganizationBalanceBindings},
		{"credit-updates", migrateCreditUpdates},
		{"bank-payments-auto-apply-errors", migrateBankPaymentAutoApplyErrors},
	}

	for _, migration := range migrations {
		log.Printf("\n\nStarting migration: %s", migration.name)
		if err := migration.fn(ctx, mdb, mysql); err != nil {
			return fmt.Errorf("migration %s failed: %w", migration.name, err)
		}
		log.Printf("Completed migration: %s", migration.name)
	}

	return nil
}

func mongoCount(ctx context.Context, db *mongo.Database, collection string) int64 {
	count, err := db.Collection(collection).CountDocuments(ctx, bson.M{})
	if err != nil {
		log.Printf("WARNING: Could not count %s: %v", collection, err)
		return 0
	}
	return count
}

func mysqlCount(db Database, table string) int64 {
	var count int64
	if err := db.GetDB().Table(table).Count(&count).Error; err != nil {
		log.Printf("WARNING: Could not count %s: %v", table, err)
		return 0
	}
	return count
}

// checkRecordExists checks if a record with the given ID exists in MySQL
func checkRecordExists(db Database, table, id string) bool {
	var count int64
	if err := db.GetDB().Table(table).Where("id = ?", id).Count(&count).Error; err != nil {
		log.Printf("WARNING: Could not check existence of %s with id %s: %v", table, id, err)
		return false
	}
	return count > 0
}

// validateDateTime validates and fixes datetime values for MySQL compatibility
func validateDateTime(t time.Time) *time.Time {
	// Check for zero time or invalid dates
	if t.IsZero() || t.Year() < 1970 || t.Year() > 2100 || t.Year() == 0 {
		return nil
	}
	return &t
}

func migrateServices(ctx context.Context, mdb *mongo.Database, mysql Database) error {
	coll := mdb.Collection("services")
	srcCount := mongoCount(ctx, mdb, "services")
	dstBefore := mysqlCount(mysql, (&Service{}).TableName())
	log.Printf("[services] mongo=%d mysql_before=%d", srcCount, dstBefore)

	cur, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	db := mysql.GetDB()
	moved := 0
	skipped := 0
	for cur.Next(ctx) {
		var s mongoService
		if err := cur.Decode(&s); err != nil {
			log.Printf("ERROR decode service: %v", err)
			return err
		}

		serviceID := s.ID.Hex()

		// Check if service already exists in MySQL
		if checkRecordExists(mysql, (&Service{}).TableName(), serviceID) {
			skipped++
			continue
		}

		service := Service{
			ID:        serviceID,
			CreatedAt: s.CreatedAt,
			Name:      s.Name,
			Code:      s.Code,
		}

		if err := db.Create(&service).Error; err != nil {
			log.Printf("ERROR insert service %s: %v", serviceID, err)
			return fmt.Errorf("service %s insert failed: %w", serviceID, err)
		}
		moved++
	}

	dstAfter := mysqlCount(mysql, (&Service{}).TableName())
	log.Printf("[services] moved=%d skipped=%d mysql_after=%d", moved, skipped, dstAfter)
	return nil
}

func migrateOrganizations(ctx context.Context, mdb *mongo.Database, mysql Database) error {
	coll := mdb.Collection("organizations")
	srcCount := mongoCount(ctx, mdb, "organizations")
	dstBefore := mysqlCount(mysql, (&Organization{}).TableName())
	demoUsesBefore := mysqlCount(mysql, (&OrganizationServiceDemoUses{}).TableName())
	log.Printf("[organizations] mongo=%d mysql_before=%d", srcCount, dstBefore)
	log.Printf("[service_demo_uses] mysql_before=%d", demoUsesBefore)

	cur, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	db := mysql.GetDB()
	moved := 0
	skipped := 0
	demoUsesMoved := 0
	for cur.Next(ctx) {
		var o mongoOrganization
		if err := cur.Decode(&o); err != nil {
			log.Printf("ERROR decode organization: %v", err)
			return err
		}

		orgID := o.ID.Hex()

		// Check if organization already exists in MySQL
		if checkRecordExists(mysql, (&Organization{}).TableName(), orgID) {
			skipped++
			// Still migrate service demo uses for existing organizations
			for _, s := range o.ServiceDemoUses {
				demo := OrganizationServiceDemoUses{
					OrganizationId: orgID,
					ServiceCode:    s.Code,
					UsedAt:         o.CreatedAt,
				}
				if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&demo).Error; err != nil {
					log.Printf("ERROR insert service_demo_use org=%s service=%s: %v", orgID, s.Code, err)
					return fmt.Errorf("org %s service_demo_use %s insert failed: %w", orgID, s.Code, err)
				}
				demoUsesMoved++
			}
			continue
		}

		org := Organization{
			ID:        orgID,
			CreatedAt: o.CreatedAt,
			UpdatedAt: o.UpdatedAt,
			DeletedAt: func() *time.Time {
				if o.DeletedAt != nil {
					return validateDateTime(*o.DeletedAt)
				}
				return nil
			}(),
			IsDeleted:                    o.IsDeleted,
			Name:                         o.Name,
			Inn:                          o.Inn,
			Pinfl:                        o.Pinfl,
			Balance:                      o.Balance,
			FiscalizationBalance:         o.FiscalizationBalance,
			ReservedFiscalizationBalance: o.ReservedFiscalizationBalance,
			TotalPayments:                o.TotalPayments,
			CreditAmount:                 o.CreditAmount,
			OrganizationCode:             o.OrganizationCode,
			ReferralAgentCode:            o.ReferralAgentCode,
			WhiteLabel:                   o.WhiteLabel,
			OfferNumber:                  o.OfferInfo.Number,
			OfferDate: func() *time.Time {
				if o.OfferInfo.Date != nil {
					return validateDateTime(*o.OfferInfo.Date)
				}
				return nil
			}(),
		}

		if err := db.Create(&org).Error; err != nil {
			log.Printf("ERROR insert organization %s: %v", orgID, err)
			return fmt.Errorf("organization %s insert failed: %w", orgID, err)
		}

		// Migrate service demo uses
		for _, s := range o.ServiceDemoUses {
			demo := OrganizationServiceDemoUses{
				OrganizationId: orgID,
				ServiceCode:    s.Code,
				UsedAt:         o.CreatedAt,
			}
			if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&demo).Error; err != nil {
				log.Printf("ERROR insert service_demo_use org=%s service=%s: %v", orgID, s.Code, err)
				return fmt.Errorf("org %s service_demo_use %s insert failed: %w", orgID, s.Code, err)
			}
			demoUsesMoved++
		}

		moved++
	}

	dstAfter := mysqlCount(mysql, (&Organization{}).TableName())
	demoUsesAfter := mysqlCount(mysql, (&OrganizationServiceDemoUses{}).TableName())
	log.Printf("[organizations] moved=%d skipped=%d mysql_after=%d", moved, skipped, dstAfter)
	log.Printf("[service_demo_uses] moved=%d mysql_after=%d", demoUsesMoved, demoUsesAfter)
	return nil
}

func migratePackages(ctx context.Context, mdb *mongo.Database, mysql Database) error {
	coll := mdb.Collection("packages")
	srcCount := mongoCount(ctx, mdb, "packages")
	dstBefore := mysqlCount(mysql, (&Package{}).TableName())
	itemsBefore := mysqlCount(mysql, (&PackageItem{}).TableName())
	bonusBefore := mysqlCount(mysql, (&PackageActivationBonusPackage{}).TableName())
	log.Printf("[packages] mongo=%d mysql_before=%d", srcCount, dstBefore)
	log.Printf("[package_items] mysql_before=%d", itemsBefore)
	log.Printf("[package_activation_bonus_packages] mysql_before=%d", bonusBefore)

	cur, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	db := mysql.GetDB()
	moved := 0
	skipped := 0
	itemsMoved := 0
	bonusMoved := 0
	for cur.Next(ctx) {
		var p mongoPackage
		if err := cur.Decode(&p); err != nil {
			log.Printf("ERROR decode package: %v", err)
			return err
		}

		pkgID := p.ID.Hex()

		// Check if package already exists in MySQL
		if checkRecordExists(mysql, (&Package{}).TableName(), pkgID) {
			skipped++
			// Still migrate package items and bonus packages for existing packages
			for _, item := range p.Items {
				pkgItem := PackageItem{
					PackageId:          pkgID,
					Name:               item.Name,
					Code:               item.Code,
					IsOverLimitAllowed: item.IsOverLimitAllowed,
					OverLimitPrice:     item.OverLimitPrice,
					BRVRate:            item.BRVRate,
					IsUnlimited:        item.IsUnlimited,
					Limit:              item.Limit,
				}
				if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&pkgItem).Error; err != nil {
					log.Printf("ERROR insert package_item pkg=%s item=%d: %v", pkgID, item.Code, err)
					return fmt.Errorf("package %s item %d insert failed: %w", pkgID, item.Code, err)
				}
				itemsMoved++
			}

			for _, bonus := range p.OnActivationBonusPackages {
				bonusPkg := PackageActivationBonusPackage{
					PackageId:      pkgID,
					BonusPackageId: bonus.ID.Hex(),
				}
				if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&bonusPkg).Error; err != nil {
					log.Printf("ERROR insert package_activation_bonus pkg=%s bonus=%s: %v", pkgID, bonus.ID.Hex(), err)
					return fmt.Errorf("package %s bonus %s insert failed: %w", pkgID, bonus.ID.Hex(), err)
				}
				bonusMoved++
			}
			continue
		}

		pkg := Package{
			ID:                          pkgID,
			CreatedAt:                   p.CreatedAt,
			IsDeleted:                   p.IsDeleted,
			Name:                        p.Name,
			Price:                       p.Price,
			BRVRate:                     p.BRVRate,
			DurationDays:                p.DurationDays,
			DurationMonths:              p.DurationMonths,
			IsDemo:                      p.IsDemo,
			IsPublic:                    p.IsPublic,
			ServiceCode:                 p.Service.Code,
			DefaultSetOnNewOrganization: p.DefaultSetOnNewOrganization,
		}

		if err := db.Create(&pkg).Error; err != nil {
			log.Printf("ERROR insert package %s: %v", pkgID, err)
			return fmt.Errorf("package %s insert failed: %w", pkgID, err)
		}

		// Migrate package items
		for _, item := range p.Items {
			pkgItem := PackageItem{
				PackageId:          pkgID,
				Name:               item.Name,
				Code:               item.Code,
				IsOverLimitAllowed: item.IsOverLimitAllowed,
				OverLimitPrice:     item.OverLimitPrice,
				BRVRate:            item.BRVRate,
				IsUnlimited:        item.IsUnlimited,
				Limit:              item.Limit,
			}
			if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&pkgItem).Error; err != nil {
				log.Printf("ERROR insert package_item pkg=%s item=%d: %v", pkgID, item.Code, err)
				return fmt.Errorf("package %s item %d insert failed: %w", pkgID, item.Code, err)
			}
			itemsMoved++
		}

		// Migrate activation bonus packages
		for _, bonus := range p.OnActivationBonusPackages {
			bonusPkg := PackageActivationBonusPackage{
				PackageId:      pkgID,
				BonusPackageId: bonus.ID.Hex(),
			}
			if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&bonusPkg).Error; err != nil {
				log.Printf("ERROR insert package_activation_bonus pkg=%s bonus=%s: %v", pkgID, bonus.ID.Hex(), err)
				return fmt.Errorf("package %s bonus %s insert failed: %w", pkgID, bonus.ID.Hex(), err)
			}
			bonusMoved++
		}

		moved++
	}

	dstAfter := mysqlCount(mysql, (&Package{}).TableName())
	itemsAfter := mysqlCount(mysql, (&PackageItem{}).TableName())
	bonusAfter := mysqlCount(mysql, (&PackageActivationBonusPackage{}).TableName())
	log.Printf("[packages] moved=%d skipped=%d mysql_after=%d", moved, skipped, dstAfter)
	log.Printf("[package_items] moved=%d mysql_after=%d", itemsMoved, itemsAfter)
	log.Printf("[package_activation_bonus_packages] moved=%d mysql_after=%d", bonusMoved, bonusAfter)
	return nil
}

func migrateBoughtPackages(ctx context.Context, mdb *mongo.Database, mysql Database) error {
	coll := mdb.Collection("boughtPackages")
	srcCount := mongoCount(ctx, mdb, "boughtPackages")
	dstBefore := mysqlCount(mysql, (&BoughtPackage{}).TableName())
	itemsBefore := mysqlCount(mysql, (&BoughtPackageItem{}).TableName())
	log.Printf("[bought-packages] mongo=%d mysql_before=%d", srcCount, dstBefore)
	log.Printf("[bought-package-items] mysql_before=%d", itemsBefore)

	cur, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	db := mysql.GetDB()
	moved := 0
	skipped := 0
	itemsMoved := 0
	for cur.Next(ctx) {
		var bp struct {
			ID           primitive.ObjectID `bson:"_id"`
			Organization struct {
				ID   primitive.ObjectID `bson:"_id"`
				Name string             `bson:"name"`
				Inn  string             `bson:"inn"`
			} `bson:"organization"`
			Package struct {
				ID           primitive.ObjectID `bson:"_id"`
				Name         string             `bson:"name"`
				Price        float64            `bson:"price"`
				IsDemo       bool               `bson:"is_demo"`
				PackageItems []struct {
					Name               string  `bson:"name"`
					Code               int     `bson:"code"`
					IsOverLimitAllowed bool    `bson:"is_over_limit_allowed"`
					OverLimitPrice     float64 `bson:"over_limit_price"`
					IsUnlimited        bool    `bson:"is_unlimited"`
					LimitValue         int     `bson:"limit"`
					UsedCount          int     `bson:"used_count"`
				} `bson:"package_items"`
			} `bson:"package"`
			BoughtAt     time.Time `bson:"bought_at"`
			ExpiresAt    time.Time `bson:"expires_at"`
			IsAutoExtend bool      `bson:"is_auto_extend"`
			IsDeleted    bool      `bson:"is_deleted"`
			Price        float64   `bson:"price"`
		}
		if err := cur.Decode(&bp); err != nil {
			log.Printf("ERROR decode bought-package: %v", err)
			return err
		}

		boughtPkgID := bp.ID.Hex()

		// Check if bought-package already exists in MySQL
		if checkRecordExists(mysql, (&BoughtPackage{}).TableName(), boughtPkgID) {
			skipped++
			continue
		}

		boughtPkg := BoughtPackage{
			ID:             boughtPkgID,
			OrganizationId: bp.Organization.ID.Hex(),
			PackageId:      bp.Package.ID.Hex(),
			BoughtAt:       bp.BoughtAt,
			ExpiresAt:      bp.ExpiresAt,
			IsAutoExtend:   bp.IsAutoExtend,
			IsActive:       !bp.IsDeleted,
			Price:          bp.Package.Price,
		}

		if err := db.Create(&boughtPkg).Error; err != nil {
			log.Printf("ERROR insert bought-package %s: %v", boughtPkgID, err)
			return fmt.Errorf("bought-package %s insert failed: %w", boughtPkgID, err)
		}
		moved++

		// Migrate package items for this bought package
		for _, item := range bp.Package.PackageItems {
			boughtPkgItemID := primitive.NewObjectID().Hex()
			boughtPkgItem := BoughtPackageItem{
				ID:                 boughtPkgItemID,
				BoughtPackageId:    boughtPkgID,
				Name:               item.Name,
				Code:               item.Code,
				IsOverLimitAllowed: item.IsOverLimitAllowed,
				OverLimitPrice:     item.OverLimitPrice,
				IsUnlimited:        item.IsUnlimited,
				LimitValue:         item.LimitValue,
				UsedCount:          item.UsedCount,
			}

			if err := db.Create(&boughtPkgItem).Error; err != nil {
				log.Printf("ERROR insert bought-package-item %s: %v", boughtPkgItemID, err)
				return fmt.Errorf("bought-package-item %s insert failed: %w", boughtPkgItemID, err)
			}
			itemsMoved++
		}
	}

	dstAfter := mysqlCount(mysql, (&BoughtPackage{}).TableName())
	itemsAfter := mysqlCount(mysql, (&BoughtPackageItem{}).TableName())
	log.Printf("[bought-packages] moved=%d skipped=%d mysql_after=%d", moved, skipped, dstAfter)
	log.Printf("[bought-package-items] moved=%d mysql_after=%d", itemsMoved, itemsAfter)
	return nil
}

func migrateCharges(ctx context.Context, mdb *mongo.Database, mysql Database) error {
	coll := mdb.Collection("charges")
	srcCount := mongoCount(ctx, mdb, "charges")
	dstBefore := mysqlCount(mysql, (&Charge{}).TableName())
	log.Printf("[charges] mongo=%d mysql_before=%d", srcCount, dstBefore)

	cur, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	db := mysql.GetDB()
	moved := 0
	skipped := 0
	for cur.Next(ctx) {
		var c struct {
			ID           primitive.ObjectID `bson:"_id"`
			CreatedAt    time.Time          `bson:"created_at"`
			IsDeleted    bool               `bson:"is_deleted"`
			Organization struct {
				ID   primitive.ObjectID `bson:"_id"`
				Name string             `bson:"name"`
				Inn  string             `bson:"inn"`
			} `bson:"organization"`
			Price   float64 `bson:"price"`
			Package struct {
				ID   primitive.ObjectID `bson:"_id"`
				Name string             `bson:"name"`
				Code int                `bson:"code"`
			} `bson:"package"`
			Service struct {
				Code string `bson:"code"`
			} `bson:"service"`
			Item struct {
				Name               string  `bson:"name"`
				Code               int     `bson:"code"`
				IsOverLimitAllowed bool    `bson:"is_over_limit_allowed"`
				OverLimitPrice     float64 `bson:"over_limit_price"`
				IsUnlimited        bool    `bson:"is_unlimited"`
				Limit              int     `bson:"limit"`
			} `bson:"item"`
			EDIReturnInvoice       *map[string]interface{} `bson:"edi_return_invoice"`
			EDIAttorney            *map[string]interface{} `bson:"edi_attorney"`
			RoamingInvoice         *map[string]interface{} `bson:"roaming_invoice"`
			RoamingContract        *map[string]interface{} `bson:"roaming_contract"`
			RoamingWaybill         *map[string]interface{} `bson:"roaming_waybill"`
			RoamingAct             *map[string]interface{} `bson:"roaming_act"`
			RoamingVerificationAct *map[string]interface{} `bson:"roaming_verification_act"`
			RoamingEmpowerment     *map[string]interface{} `bson:"roaming_empowerment"`
		}
		if err := cur.Decode(&c); err != nil {
			log.Printf("ERROR decode charge: %v", err)
			return err
		}

		chargeID := c.ID.Hex()

		// Check if charge already exists in MySQL
		if checkRecordExists(mysql, (&Charge{}).TableName(), chargeID) {
			skipped++
			continue
		}

		// Determine charge type based on which document fields are present
		chargeType := 0
		var objectId, number string
		var date1, date2 *time.Time

		// Debug: log the charge structure to understand what we're working with
		log.Printf("DEBUG: Processing charge %s, RoamingInvoice: %v, RoamingContract: %v", chargeID, c.RoamingInvoice != nil, c.RoamingContract != nil)

		// Check for different document types and set the appropriate type
		if c.RoamingInvoice != nil {
			chargeType = 3 // RoamingInvoiceType
			if id, ok := (*c.RoamingInvoice)["_id"].(string); ok {
				objectId = id
			}
			if num, ok := (*c.RoamingInvoice)["number"].(string); ok {
				number = num
			}
			if date, ok := (*c.RoamingInvoice)["date"].(time.Time); ok {
				date1 = &date
			} else {
				// Try to parse as string if time.Time assertion fails
				if dateStr, ok := (*c.RoamingInvoice)["date"].(string); ok {
					if parsedDate, err := time.Parse(time.RFC3339, dateStr); err == nil {
						date1 = &parsedDate
					}
				}
			}
		} else if c.RoamingContract != nil {
			chargeType = 7 // RoamingContractType
			if id, ok := (*c.RoamingContract)["_id"].(string); ok {
				objectId = id
			}
			if num, ok := (*c.RoamingContract)["number"].(string); ok {
				number = num
			}
			if date, ok := (*c.RoamingContract)["date"].(time.Time); ok {
				date1 = &date
			}
		} else if c.RoamingWaybill != nil {
			chargeType = 10 // RoamingWaybillType
			if id, ok := (*c.RoamingWaybill)["_id"].(string); ok {
				objectId = id
			}
			if num, ok := (*c.RoamingWaybill)["number"].(string); ok {
				number = num
			}
			if date, ok := (*c.RoamingWaybill)["date"].(time.Time); ok {
				date1 = &date
			}
		} else if c.RoamingAct != nil {
			chargeType = 9 // RoamingActType
			if id, ok := (*c.RoamingAct)["_id"].(string); ok {
				objectId = id
			}
			if num, ok := (*c.RoamingAct)["number"].(string); ok {
				number = num
			}
			if date, ok := (*c.RoamingAct)["date"].(time.Time); ok {
				date1 = &date
			}
		} else if c.RoamingVerificationAct != nil {
			chargeType = 8 // RoamingVerificationActType
			if id, ok := (*c.RoamingVerificationAct)["_id"].(string); ok {
				objectId = id
			}
			if num, ok := (*c.RoamingVerificationAct)["number"].(string); ok {
				number = num
			}
			if date, ok := (*c.RoamingVerificationAct)["date"].(time.Time); ok {
				date1 = &date
			}
		} else if c.RoamingEmpowerment != nil {
			chargeType = 11 // RoamingEmpowermentType
			if id, ok := (*c.RoamingEmpowerment)["_id"].(string); ok {
				objectId = id
			}
			if num, ok := (*c.RoamingEmpowerment)["number"].(string); ok {
				number = num
			}
			if startDate, ok := (*c.RoamingEmpowerment)["start_date"].(time.Time); ok {
				date1 = &startDate
			}
			if endDate, ok := (*c.RoamingEmpowerment)["end_date"].(time.Time); ok {
				date2 = &endDate
			}
		} else if c.EDIReturnInvoice != nil {
			chargeType = 2 // EDIReturnInvoiceType
			if id, ok := (*c.EDIReturnInvoice)["_id"].(string); ok {
				objectId = id
			}
			if num, ok := (*c.EDIReturnInvoice)["number"].(string); ok {
				number = num
			}
			if date, ok := (*c.EDIReturnInvoice)["date"].(time.Time); ok {
				date1 = &date
			}
		} else if c.EDIAttorney != nil {
			chargeType = 4 // EDIAttorneyType
			if id, ok := (*c.EDIAttorney)["_id"].(string); ok {
				objectId = id
			}
			if num, ok := (*c.EDIAttorney)["number"].(string); ok {
				number = num
			}
			if startDate, ok := (*c.EDIAttorney)["start_date"].(time.Time); ok {
				date1 = &startDate
			}
			if endDate, ok := (*c.EDIAttorney)["end_date"].(time.Time); ok {
				date2 = &endDate
			}
		}

		// If no dates were found from document fields, use created_at as fallback
		if date1 == nil {
			date1 = &c.CreatedAt
		}

		charge := Charge{
			ID:                    chargeID,
			CreatedAt:             c.CreatedAt,
			IsDeleted:             c.IsDeleted,
			OrganizationId:        c.Organization.ID.Hex(),
			Price:                 c.Price,
			Type:                  chargeType,
			BoughtPackageID:       c.Package.ID.Hex(),
			BoughtPackageItemCode: c.Item.Code,
			ServiceCode:           c.Service.Code,
			ObjectId:              objectId,
			Number:                number,
			Date1: func() *time.Time {
				if date1 != nil {
					return validateDateTime(*date1)
				}
				return nil
			}(),
			Date2: func() *time.Time {
				if date2 != nil {
					return validateDateTime(*date2)
				}
				return nil
			}(),
		}

		if err := db.Create(&charge).Error; err != nil {
			log.Printf("ERROR insert charge %s: %v", chargeID, err)
			return fmt.Errorf("charge %s insert failed: %w", chargeID, err)
		}
		moved++
	}

	dstAfter := mysqlCount(mysql, (&Charge{}).TableName())
	log.Printf("[charges] moved=%d skipped=%d mysql_after=%d", moved, skipped, dstAfter)
	return nil
}

func migratePayments(ctx context.Context, mdb *mongo.Database, mysql Database) error {
	coll := mdb.Collection("payments")
	srcCount := mongoCount(ctx, mdb, "payments")
	dstBefore := mysqlCount(mysql, (&Payment{}).TableName())
	log.Printf("[payments] mongo=%d mysql_before=%d", srcCount, dstBefore)

	cur, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	db := mysql.GetDB()
	moved := 0
	skipped := 0
	for cur.Next(ctx) {
		var p struct {
			ID           primitive.ObjectID `bson:"_id"`
			CreatedAt    time.Time          `bson:"created_at"`
			Amount       float64            `bson:"amount"`
			Organization struct {
				ID   primitive.ObjectID `bson:"_id"`
				Name string             `bson:"name"`
				Inn  string             `bson:"inn"`
			} `bson:"organization"`
			Account struct {
				ID       primitive.ObjectID `bson:"_id"`
				Name     string             `bson:"name"`
				Username string             `bson:"username"`
			} `bson:"account"`
			Method            int     `bson:"method"`
			BankTransactionID *string `bson:"bank_transaction_id"`
		}
		if err := cur.Decode(&p); err != nil {
			log.Printf("ERROR decode payment: %v", err)
			return err
		}

		paymentID := p.ID.Hex()

		// Check if payment already exists in MySQL
		if checkRecordExists(mysql, (&Payment{}).TableName(), paymentID) {
			skipped++
			continue
		}

		payment := Payment{
			ID:                paymentID,
			CreatedAt:         p.CreatedAt,
			Amount:            p.Amount,
			OrganizationID:    p.Organization.ID.Hex(),
			AccountID:         p.Account.ID.Hex(),
			Method:            p.Method,
			BankTransactionID: p.BankTransactionID,
		}

		if err := db.Create(&payment).Error; err != nil {
			log.Printf("ERROR insert payment %s: %v", paymentID, err)
			return fmt.Errorf("payment %s insert failed: %w", paymentID, err)
		}
		moved++
	}

	dstAfter := mysqlCount(mysql, (&Payment{}).TableName())
	log.Printf("[payments] moved=%d skipped=%d mysql_after=%d", moved, skipped, dstAfter)
	return nil
}

func migratePaymeTransactions(ctx context.Context, mdb *mongo.Database, mysql Database) error {
	coll := mdb.Collection("paymeTransactions")
	srcCount := mongoCount(ctx, mdb, "paymeTransactions")
	dstBefore := mysqlCount(mysql, (&PaymeTransaction{}).TableName())
	log.Printf("[payme-transactions] mongo=%d mysql_before=%d", srcCount, dstBefore)

	cur, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	db := mysql.GetDB()
	moved := 0
	skipped := 0
	for cur.Next(ctx) {
		var pt struct {
			ID                 primitive.ObjectID `bson:"_id"`
			CreatedAt          time.Time          `bson:"created_at"`
			PaymeTransactionID string             `bson:"payme_transaction_id"`
			PaymeCreatedAt     time.Time          `bson:"payme_created_at"`
			SystemCompletedAt  *time.Time         `bson:"system_completed_at"`
			State              int                `bson:"state"`
			Amount             float64            `bson:"amount"`
			PaymentId          *string            `bson:"payment_id"`
			Organization       struct {
				ID   primitive.ObjectID `bson:"_id"`
				Name string             `bson:"name"`
				Inn  string             `bson:"inn"`
			} `bson:"organization"`
			Reason           int        `bson:"reason"`
			SystemCanceledAt *time.Time `bson:"system_canceled_at"`
		}
		if err := cur.Decode(&pt); err != nil {
			log.Printf("ERROR decode payme-transaction: %v", err)
			return err
		}

		paymeTransactionID := pt.ID.Hex()

		// Check if payme-transaction already exists in MySQL
		if checkRecordExists(mysql, (&PaymeTransaction{}).TableName(), paymeTransactionID) {
			skipped++
			continue
		}

		// Validate PaymeCreatedAt - if invalid, use CreatedAt as fallback
		validatedPaymeCreatedAt := validateDateTime(pt.PaymeCreatedAt)
		if validatedPaymeCreatedAt == nil {
			// Use CreatedAt as fallback, but validate it too
			validatedCreatedAt := validateDateTime(pt.CreatedAt)
			if validatedCreatedAt != nil {
				validatedPaymeCreatedAt = validatedCreatedAt
			} else {
				// If both are invalid, use current time
				now := time.Now()
				validatedPaymeCreatedAt = &now
			}
		}

		paymeTransaction := PaymeTransaction{
			ID:                 paymeTransactionID,
			CreatedAt:          pt.CreatedAt,
			PaymeTransactionID: pt.PaymeTransactionID,
			PaymeCreatedAt:     *validatedPaymeCreatedAt,
			SystemCompletedAt: func() *time.Time {
				if pt.SystemCompletedAt != nil {
					return validateDateTime(*pt.SystemCompletedAt)
				}
				return nil
			}(),
			State:          pt.State,
			Amount:         pt.Amount,
			PaymentId:      pt.PaymentId,
			OrganizationID: pt.Organization.ID.Hex(),
			Reason:         pt.Reason,
			SystemCanceledAt: func() *time.Time {
				if pt.SystemCanceledAt != nil {
					return validateDateTime(*pt.SystemCanceledAt)
				}
				return nil
			}(),
		}

		if err := db.Create(&paymeTransaction).Error; err != nil {
			log.Printf("ERROR insert payme-transaction %s: %v", paymeTransactionID, err)
			return fmt.Errorf("payme-transaction %s insert failed: %w", paymeTransactionID, err)
		}
		moved++
	}

	dstAfter := mysqlCount(mysql, (&PaymeTransaction{}).TableName())
	log.Printf("[payme-transactions] moved=%d skipped=%d mysql_after=%d", moved, skipped, dstAfter)
	return nil
}

func migrateOrganizationBalanceBindings(ctx context.Context, mdb *mongo.Database, mysql Database) error {
	coll := mdb.Collection("organizationBalanceBindings")
	srcCount := mongoCount(ctx, mdb, "organizationBalanceBindings")
	dstBefore := mysqlCount(mysql, (&OrganizationBalanceBinding{}).TableName())
	log.Printf("[organization-balance-bindings] mongo=%d mysql_before=%d", srcCount, dstBefore)

	cur, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	db := mysql.GetDB()
	moved := 0
	skipped := 0
	for cur.Next(ctx) {
		var obb struct {
			ID                primitive.ObjectID `bson:"_id"`
			CreatedAt         time.Time          `bson:"created_at"`
			DeletedAt         *time.Time         `bson:"deleted_at"`
			IsDeleted         bool               `bson:"is_deleted"`
			PayerOrganization struct {
				ID   primitive.ObjectID `bson:"id"`
				Name string             `bson:"name"`
				Inn  string             `bson:"inn"`
			} `bson:"payer_organization"`
			TargetOrganization struct {
				ID   primitive.ObjectID `bson:"id"`
				Name string             `bson:"name"`
				Inn  string             `bson:"inn"`
			} `bson:"target_organization"`
		}
		if err := cur.Decode(&obb); err != nil {
			log.Printf("ERROR decode organization-balance-binding: %v", err)
			return err
		}

		orgBalanceBindingID := obb.ID.Hex()

		// Check if organization-balance-binding already exists in MySQL
		if checkRecordExists(mysql, (&OrganizationBalanceBinding{}).TableName(), orgBalanceBindingID) {
			skipped++
			continue
		}

		orgBalanceBinding := OrganizationBalanceBinding{
			ID:        orgBalanceBindingID,
			CreatedAt: obb.CreatedAt,
			DeletedAt: func() *time.Time {
				if obb.DeletedAt != nil {
					return validateDateTime(*obb.DeletedAt)
				}
				return nil
			}(),
			IsDeleted:              obb.IsDeleted,
			PayerOrganizationID:    obb.PayerOrganization.ID.Hex(),
			TargetOrganizationID:   obb.TargetOrganization.ID.Hex(),
			PayerOrganizationName:  obb.PayerOrganization.Name,
			TargetOrganizationName: obb.TargetOrganization.Name,
		}

		if err := db.Create(&orgBalanceBinding).Error; err != nil {
			log.Printf("ERROR insert organization-balance-binding %s: %v", orgBalanceBindingID, err)
			return fmt.Errorf("organization-balance-binding %s insert failed: %w", orgBalanceBindingID, err)
		}
		moved++
	}

	dstAfter := mysqlCount(mysql, (&OrganizationBalanceBinding{}).TableName())
	log.Printf("[organization-balance-bindings] moved=%d skipped=%d mysql_after=%d", moved, skipped, dstAfter)
	return nil
}

func migrateCreditUpdates(ctx context.Context, mdb *mongo.Database, mysql Database) error {
	coll := mdb.Collection("creditUpdates")
	srcCount := mongoCount(ctx, mdb, "creditUpdates")
	dstBefore := mysqlCount(mysql, (&CreditUpdates{}).TableName())
	log.Printf("[credit-updates] mongo=%d mysql_before=%d", srcCount, dstBefore)

	cur, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	db := mysql.GetDB()
	moved := 0
	skipped := 0
	for cur.Next(ctx) {
		var cu struct {
			ID           primitive.ObjectID `bson:"_id"`
			CreatedAt    time.Time          `bson:"created_at"`
			Organization struct {
				ID   primitive.ObjectID `bson:"_id"`
				Name string             `bson:"name"`
				Inn  string             `bson:"inn"`
			} `bson:"organization"`
			Amount  float64 `bson:"amount"`
			Account struct {
				ID       primitive.ObjectID `bson:"_id"`
				Name     string             `bson:"name"`
				Username string             `bson:"username"`
			} `bson:"account"`
		}
		if err := cur.Decode(&cu); err != nil {
			log.Printf("ERROR decode credit-update: %v", err)
			return err
		}

		creditUpdateID := cu.ID.Hex()

		// Check if credit-update already exists in MySQL
		if checkRecordExists(mysql, (&CreditUpdates{}).TableName(), creditUpdateID) {
			skipped++
			continue
		}

		creditUpdate := CreditUpdates{
			ID:             creditUpdateID,
			CreatedAt:      cu.CreatedAt,
			OrganizationID: cu.Organization.ID.Hex(),
			Amount:         cu.Amount,
			AccountID:      cu.Account.ID.Hex(),
		}

		if err := db.Create(&creditUpdate).Error; err != nil {
			log.Printf("ERROR insert credit-update %s: %v", creditUpdateID, err)
			return fmt.Errorf("credit-update %s insert failed: %w", creditUpdateID, err)
		}
		moved++
	}

	dstAfter := mysqlCount(mysql, (&CreditUpdates{}).TableName())
	log.Printf("[credit-updates] moved=%d skipped=%d mysql_after=%d", moved, skipped, dstAfter)
	return nil
}

func migrateBankPaymentAutoApplyErrors(ctx context.Context, mdb *mongo.Database, mysql Database) error {
	coll := mdb.Collection("bankPaymentsAutoApplyErrors")
	srcCount := mongoCount(ctx, mdb, "bankPaymentsAutoApplyErrors")
	dstBefore := mysqlCount(mysql, (&BankPaymentAutoApplyError{}).TableName())
	log.Printf("[bank-payments-auto-apply-errors] mongo=%d mysql_before=%d", srcCount, dstBefore)

	cur, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	db := mysql.GetDB()
	moved := 0
	skipped := 0
	for cur.Next(ctx) {
		var bpae struct {
			ID            primitive.ObjectID `bson:"_id"`
			CreatedAt     time.Time          `bson:"created_at"`
			ErrorMessage  string             `bson:"error_message"`
			Amount        float64            `bson:"amount"`
			TransactionID string             `bson:"transaction_id"`
			PayerInn      string             `bson:"payer_inn"`
			PayerName     string             `bson:"payer_name"`
			Description   *string            `bson:"description"`
			Resolved      bool               `bson:"resolved"`
		}
		if err := cur.Decode(&bpae); err != nil {
			log.Printf("ERROR decode bank-payment-auto-apply-error: %v", err)
			return err
		}

		bankPaymentAutoApplyErrorID := bpae.ID.Hex()

		// Check if bank-payment-auto-apply-error already exists in MySQL
		if checkRecordExists(mysql, (&BankPaymentAutoApplyError{}).TableName(), bankPaymentAutoApplyErrorID) {
			skipped++
			continue
		}

		bankPaymentAutoApplyError := BankPaymentAutoApplyError{
			ID:            bankPaymentAutoApplyErrorID,
			CreatedAt:     bpae.CreatedAt,
			ErrorMessage:  bpae.ErrorMessage,
			Amount:        bpae.Amount,
			TransactionID: bpae.TransactionID,
			PayerInn:      bpae.PayerInn,
			PayerName:     bpae.PayerName,
			Description:   bpae.Description,
			Resolved:      bpae.Resolved,
		}

		if err := db.Create(&bankPaymentAutoApplyError).Error; err != nil {
			log.Printf("ERROR insert bank-payment-auto-apply-error %s: %v", bankPaymentAutoApplyErrorID, err)
			return fmt.Errorf("bank-payment-auto-apply-error %s insert failed: %w", bankPaymentAutoApplyErrorID, err)
		}
		moved++
	}

	dstAfter := mysqlCount(mysql, (&BankPaymentAutoApplyError{}).TableName())
	log.Printf("[bank-payments-auto-apply-errors] moved=%d skipped=%d mysql_after=%d", moved, skipped, dstAfter)
	return nil
}
