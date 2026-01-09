package s3db

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/mulgadc/predastore/auth"
	"github.com/stretchr/testify/assert"
)

func TestSignRequest(t *testing.T) {
	tests := []struct {
		name      string
		method    string
		path      string
		accessKey string
		secretKey string
		region    string
		service   string
		wantErr   bool
	}{
		{
			name:      "Valid signing",
			method:    "GET",
			path:      "/v1/get/test-table/test-key",
			accessKey: "TESTACCESSKEY",
			secretKey: "TESTSECRETKEY",
			region:    "us-east-1",
			service:   "s3db",
			wantErr:   false,
		},
		{
			name:      "POST request",
			method:    "POST",
			path:      "/v1/put/test-table/test-key",
			accessKey: "TESTACCESSKEY",
			secretKey: "TESTSECRETKEY",
			region:    "us-east-1",
			service:   "s3db",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, "http://localhost:6660"+tt.path, nil)
			assert.NoError(t, err)

			err = SignRequest(req, tt.accessKey, tt.secretKey, tt.region, tt.service)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				// Verify the Authorization header was set
				authHeader := req.Header.Get("Authorization")
				assert.Contains(t, authHeader, "AWS4-HMAC-SHA256")
				assert.Contains(t, authHeader, tt.accessKey)
				// Verify the X-Amz-Date header was set
				dateHeader := req.Header.Get("X-Amz-Date")
				assert.NotEmpty(t, dateHeader)
			}
		})
	}
}

func TestValidateSignature(t *testing.T) {
	credentials := map[string]string{
		"TESTACCESSKEY": "TESTSECRETKEY",
	}
	region := "us-east-1"
	service := "s3db"

	tests := []struct {
		name         string
		method       string
		path         string
		setupHeaders func(req *http.Request)
		wantAccessKey string
		wantErr      bool
	}{
		{
			name:   "Missing Authorization header",
			method: "GET",
			path:   "/v1/get/test-table/test-key",
			setupHeaders: func(req *http.Request) {
				// No auth header
			},
			wantAccessKey: "",
			wantErr:       true,
		},
		{
			name:   "Valid signature",
			method: "GET",
			path:   "/v1/get/test-table/test-key",
			setupHeaders: func(req *http.Request) {
				timestamp := time.Now().UTC().Format(auth.TimeFormat)
				err := auth.GenerateAuthHeaderReq(
					"TESTACCESSKEY",
					"TESTSECRETKEY",
					timestamp,
					region,
					service,
					req,
				)
				if err != nil {
					t.Fatalf("Error generating auth header: %v", err)
				}
			},
			wantAccessKey: "TESTACCESSKEY",
			wantErr:       false,
		},
		{
			name:   "Invalid access key",
			method: "GET",
			path:   "/v1/get/test-table/test-key",
			setupHeaders: func(req *http.Request) {
				timestamp := time.Now().UTC().Format(auth.TimeFormat)
				err := auth.GenerateAuthHeaderReq(
					"INVALIDACCESSKEY",
					"TESTSECRETKEY",
					timestamp,
					region,
					service,
					req,
				)
				if err != nil {
					t.Fatalf("Error generating auth header: %v", err)
				}
			},
			wantAccessKey: "",
			wantErr:       true,
		},
		{
			name:   "Invalid signature (wrong secret)",
			method: "GET",
			path:   "/v1/get/test-table/test-key",
			setupHeaders: func(req *http.Request) {
				timestamp := time.Now().UTC().Format(auth.TimeFormat)
				err := auth.GenerateAuthHeaderReq(
					"TESTACCESSKEY",
					"WRONGSECRETKEY",
					timestamp,
					region,
					service,
					req,
				)
				if err != nil {
					t.Fatalf("Error generating auth header: %v", err)
				}
			},
			wantAccessKey: "",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create Fiber app for test
			app := fiber.New()

			var gotAccessKey string
			var gotErr error

			app.All("*", func(c *fiber.Ctx) error {
				gotAccessKey, gotErr = ValidateSignature(c, credentials, region, service)
				return c.SendString("OK")
			})

			// Create request
			req := httptest.NewRequest(tt.method, tt.path, nil)
			req.Host = "localhost:6660"
			if tt.setupHeaders != nil {
				tt.setupHeaders(req)
			}

			// Perform request
			_, err := app.Test(req)
			assert.NoError(t, err)

			if tt.wantErr {
				assert.Error(t, gotErr)
			} else {
				assert.NoError(t, gotErr)
				assert.Equal(t, tt.wantAccessKey, gotAccessKey)
			}
		})
	}
}

func TestValidateSignature_Integration(t *testing.T) {
	// Test full round-trip: sign request on client side, validate on server side
	credentials := map[string]string{
		"TESTACCESSKEY": "TESTSECRETKEY",
	}
	region := "us-east-1"
	service := "s3db"

	// Create Fiber app that uses the auth middleware
	app := fiber.New()

	app.Use(func(c *fiber.Ctx) error {
		accessKey, err := ValidateSignature(c, credentials, region, service)
		if err != nil {
			return c.Status(http.StatusForbidden).JSON(fiber.Map{
				"error":   "AccessDenied",
				"message": err.Error(),
			})
		}
		c.Locals("accessKey", accessKey)
		return c.Next()
	})

	app.Get("/v1/get/:table/:key", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":    "success",
			"accessKey": c.Locals("accessKey"),
		})
	})

	// Create a signed request
	req := httptest.NewRequest("GET", "/v1/get/test-table/test-key", nil)
	req.Host = "localhost:6660"

	timestamp := time.Now().UTC().Format(auth.TimeFormat)
	err := auth.GenerateAuthHeaderReq(
		"TESTACCESSKEY",
		"TESTSECRETKEY",
		timestamp,
		region,
		service,
		req,
	)
	assert.NoError(t, err)

	// Perform request
	resp, err := app.Test(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestDefaultConstants(t *testing.T) {
	assert.Equal(t, "us-east-1", DefaultRegion)
	assert.Equal(t, "s3db", DefaultService)
}
