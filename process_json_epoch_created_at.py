#!/usr/bin/env python3
import json
import time
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

def convert_epoch(value):
    """
    Convert an epoch value into an ISO formatted UTC timestamp.
    """
    try:
        return datetime.utcfromtimestamp(float(value)).isoformat() + "Z" if value else None
    except Exception:
        return value

def check_bigquery_connection():
    """
    Checks the BigQuery connection.
    """
    try:
        client = bigquery.Client()
        print("BigQuery connection successful.")
        return True, client
    except GoogleAPIError as e:
        print(f"BigQuery connection failed: {e}")
        return False, None

def process_json(client):
    """
    Processes JSON file and loads data into BigQuery tables.
    """
    try:
        with open('etl.json', 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Failed to read etl.json: {e}")
        return

    customer_rows = []
    subscription_rows = []
    
    for record in data.get("list", []):
        subscription = record.get("subscription", {})
        customer = record.get("customer", {})

        # Populate Customers
        customer_rows.append({
            "id": customer.get("id"),
            "first_name": customer.get("first_name"),
            "last_name": customer.get("last_name"),
            "email": customer.get("email"),
            "company": customer.get("company"),
            "auto_collection": customer.get("auto_collection"),
            "offline_payment_method": customer.get("offline_payment_method"),
            "net_term_days": customer.get("net_term_days"),
            "allow_direct_debit": customer.get("allow_direct_debit"),
            "created_at": convert_epoch(customer.get("created_at")),
            "created_from_ip": customer.get("created_from_ip"),
            "taxability": customer.get("taxability"),
            "updated_at": convert_epoch(customer.get("updated_at")),
            "channel": customer.get("channel"),
            "deleted": customer.get("deleted"),
            "preferred_currency_code": customer.get("preferred_currency_code"),
            "cf_payment_id": customer.get("cf_payment_id"),
            "billing_address": {
                "first_name": customer.get("billing_address", {}).get("first_name"),
                "last_name": customer.get("billing_address", {}).get("last_name"),
                "email": customer.get("billing_address", {}).get("email"),
                "company": customer.get("billing_address", {}).get("company"),
                "line1": customer.get("billing_address", {}).get("line1"),
                "city": customer.get("billing_address", {}).get("city"),
                "state": customer.get("billing_address", {}).get("state"),
                "state_code": customer.get("billing_address", {}).get("state_code"),
                "country": customer.get("billing_address", {}).get("country"),
                "zip": customer.get("billing_address", {}).get("zip"),
                "validation_status": customer.get("billing_address", {}).get("validation_status"),
            }
        })

        # Populate Subscriptions
        subscription_rows.append({
            "id": subscription.get("id"),
            "billing_period": subscription.get("billing_period"),
            "billing_period_unit": subscription.get("billing_period_unit"),
            "customer_id": subscription.get("customer_id"),
            "status": subscription.get("status"),
            "current_term_start": convert_epoch(subscription.get("current_term_start")),
            "current_term_end": convert_epoch(subscription.get("current_term_end")),
            "next_billing_at": convert_epoch(subscription.get("next_billing_at")),
            "created_at": convert_epoch(subscription.get("created_at")),
            "started_at": convert_epoch(subscription.get("started_at")),
            "activated_at": convert_epoch(subscription.get("activated_at")),
            "created_from_ip": subscription.get("created_from_ip"),
            "updated_at": convert_epoch(subscription.get("updated_at")),
            "has_scheduled_changes": subscription.get("has_scheduled_changes"),
            "channel": subscription.get("channel"),
            "resource_version": subscription.get("resource_version"),
            "deleted": subscription.get("deleted"),
            "coupon": subscription.get("coupon"),
            "currency_code": subscription.get("currency_code"),
            "due_invoices_count": subscription.get("due_invoices_count"),
            "due_since": convert_epoch(subscription.get("due_since")),
            "total_dues": subscription.get("total_dues"),
            "mrr": subscription.get("mrr"),
            "exchange_rate": subscription.get("exchange_rate"),
            "base_currency_code": subscription.get("base_currency_code"),
            "subscription_items": subscription.get("subscription_items", []),
            "item_tiers": subscription.get("item_tiers", []),
            "coupons": subscription.get("coupons", [])
        })

    dataset_id = "sandbox"
    project = client.project
    
    tables = [
        ("customers", customer_rows, "id"),
        ("subscriptions", subscription_rows, "id")
    ]

    for table_name, rows, dedup_key in tables:
        if rows:
            table_id = f"{project}.{dataset_id}.{table_name}"
            print(f"Inserting {len(rows)} rows into {table_name}...")
            client.insert_rows_json(table_id, rows)
            print(f"Inserted rows into {table_name} successfully.")

def main():
    connection_ok, client = check_bigquery_connection()
    if connection_ok:
        print("BigQuery connection check passed. Proceeding with JSON processing.")
        process_json(client)
    else:
        print("BigQuery connection check failed. Exiting.")

if __name__ == "__main__":
    main()
