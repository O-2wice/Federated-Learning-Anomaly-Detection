"""
Grafana Dashboard Setup Script
Automatically configures Grafana data source and creates dashboards via API

Automatically detects if running inside Docker or locally

Usage:
    python scripts/06_setup_grafana.py
"""

import requests
import json
import time
import logging
import os
import sys
from typing import Dict, Any, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import config loader for environment detection
sys.path.insert(0, os.path.dirname(__file__))
from config_loader import get_db_config, get_grafana_config

# Grafana configuration - auto-detect Docker vs local
grafana_config = get_grafana_config()
db_config = get_db_config()

GRAFANA_URL = grafana_config['url']
GRAFANA_USER = grafana_config['user']
GRAFANA_PASSWORD = grafana_config['password']

# TimescaleDB configuration - auto-detect Docker vs local
# When setting datasource, use timescaledb for both Docker and local (Docker DNS resolves it)
DB_HOST = db_config['host']
DB_PORT = db_config['port']
DB_NAME = db_config['database']
DB_USER = db_config['user']
DB_PASSWORD = db_config['password']

logger.info(f"Grafana URL: {GRAFANA_URL}")
logger.info(f"Database Config: host={DB_HOST}, port={DB_PORT}")



class GrafanaConfigurator:
    """Automated Grafana configuration"""
    
    def __init__(self):
        self.base_url = GRAFANA_URL
        self.session = requests.Session()
        self.session.auth = (GRAFANA_USER, GRAFANA_PASSWORD)
        self.datasource_uid = None
    
    def wait_for_grafana(self, max_retries: int = 30) -> bool:
        """Wait for Grafana to be ready"""
        logger.info("Waiting for Grafana to be ready...")
        
        for i in range(max_retries):
            try:
                response = self.session.get(f"{self.base_url}/api/health", timeout=5)
                if response.status_code == 200:
                    logger.info("✓ Grafana is ready")
                    return True
            except requests.exceptions.RequestException:
                pass
            
            if i < max_retries - 1:
                time.sleep(2)
        
        logger.error("✗ Grafana is not responding")
        return False
    
    def create_datasource(self) -> bool:
        """Create TimescaleDB data source"""
        logger.info("Creating TimescaleDB data source...")
        
        # Check if datasource already exists
        try:
            response = self.session.get(f"{self.base_url}/api/datasources/name/FLEAD-TimescaleDB")
            if response.status_code == 200:
                existing = response.json()
                self.datasource_uid = existing['uid']
                logger.info(f"✓ Data source already exists (UID: {self.datasource_uid})")
                return True
        except:
            pass
        
        # Create new datasource
        datasource_config = {
            "name": "FLEAD-TimescaleDB",
            "type": "postgres",
            "access": "proxy",
            "url": f"{DB_HOST}:{DB_PORT}",
            "database": DB_NAME,
            "user": DB_USER,
            "secureJsonData": {
                "password": DB_PASSWORD
            },
            "jsonData": {
                "sslmode": "disable",
                "postgresVersion": 1200,
                "timescaledb": True
            },
            "isDefault": True
        }
        
        try:
            response = self.session.post(
                f"{self.base_url}/api/datasources",
                json=datasource_config,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code in [200, 201]:
                result = response.json()
                self.datasource_uid = result.get('datasource', {}).get('uid') or result.get('uid')
                logger.info(f"✓ Data source created successfully (UID: {self.datasource_uid})")
                return True
            else:
                logger.error(f"✗ Failed to create data source: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return False
        except Exception as e:
            logger.error(f"✗ Error creating data source: {e}")
            return False
    
    LEGACY_DASHBOARD_TITLE = "FLEAD - Federated Learning Monitoring"

    def remove_legacy_dashboard(self) -> None:
        """Delete the old API-created dashboard left over from earlier runs."""
        try:
            response = self.session.get(
                f"{self.base_url}/api/search",
                params={"query": self.LEGACY_DASHBOARD_TITLE, "type": "dash-db"},
                timeout=10,
            )
            response.raise_for_status()
            for item in response.json():
                if item.get("title") == self.LEGACY_DASHBOARD_TITLE:
                    deleted = self.session.delete(
                        f"{self.base_url}/api/dashboards/uid/{item['uid']}", timeout=10
                    )
                    logger.info(
                        "Removed legacy dashboard '%s' (uid=%s, HTTP %s)",
                        item["title"], item["uid"], deleted.status_code,
                    )
        except Exception as e:
            logger.warning(f"Could not check for the legacy dashboard: {e}")

    def create_dashboard(self) -> bool:
        """Create main monitoring dashboard (legacy; dashboards are now provisioned)"""
        logger.info("Creating FLEAD monitoring dashboard...")
        
        if not self.datasource_uid:
            logger.error("✗ No datasource UID available")
            return False
        
        dashboard = None
        
        # Try to load from file first (mounted via docker-compose)
        dashboard_file = "/app/dashboards/flead_dashboard.json"
        if os.path.exists(dashboard_file):
            try:
                logger.info(f"Loading dashboard from {dashboard_file}...")
                with open(dashboard_file, 'r') as f:
                    dashboard_data = json.load(f)
                
                # Ensure the dashboard uses the correct datasource UID if possible, 
                # or rely on the name "FLEAD-TimescaleDB" matching.
                # We wrap it in the structure Grafana API expects.
                dashboard = {
                    "dashboard": dashboard_data,
                    "overwrite": True
                }
                # Nullify ID to ensure creation/overwrite works smoothly
                dashboard["dashboard"]["id"] = None
                
            except Exception as e:
                logger.error(f"Failed to load dashboard file: {e}")
                dashboard = None
        else:
            logger.warning(f"Dashboard file not found at {dashboard_file}, using hardcoded default.")

        if dashboard is None:
            dashboard = {
                "dashboard": {
                    "title": "FLEAD - Federated Learning Monitoring",
                    "tags": ["flead", "federated-learning", "iot"],
                    "timezone": "browser",
                    "refresh": "30s",
                    "time": {
                        "from": "now-1h",
                        "to": "now"
                    },
                    "panels": [
                        # Row 1: Stats
                        {
                            "id": 1,
                            "title": "Total Local Models",
                            "type": "stat",
                            "gridPos": {"h": 4, "w": 6, "x": 0, "y": 0},
                            "targets": [{
                                "datasource": {"uid": self.datasource_uid},
                                "rawSql": "SELECT COUNT(*) FROM local_models",
                                "format": "table",
                                "refId": "A"
                            }],
                            "options": {
                                "colorMode": "value",
                                "graphMode": "none",
                                "textMode": "auto"
                            },
                            "fieldConfig": {
                                "defaults": {
                                    "color": {"mode": "thresholds"},
                                    "thresholds": {
                                        "mode": "absolute",
                                        "steps": [
                                            {"value": 0, "color": "red"},
                                            {"value": 100, "color": "yellow"},
                                            {"value": 1000, "color": "green"}
                                        ]
                                    }
                                }
                            }
                        },
                        {
                            "id": 2,
                            "title": "Global Models Created",
                            "type": "stat",
                            "gridPos": {"h": 4, "w": 6, "x": 6, "y": 0},
                            "targets": [{
                                "datasource": {"uid": self.datasource_uid},
                                "rawSql": "SELECT COUNT(*) FROM federated_models",
                                "format": "table",
                                "refId": "A"
                            }],
                            "options": {
                                "colorMode": "value",
                                "graphMode": "none"
                            },
                            "fieldConfig": {
                                "defaults": {
                                    "color": {"mode": "palette-classic"}
                                }
                            }
                    },
                    {
                        "id": 3,
                        "title": "Active Devices (Last Hour)",
                        "type": "stat",
                        "gridPos": {"h": 4, "w": 6, "x": 12, "y": 0},
                        "targets": [{
                            "datasource": {"uid": self.datasource_uid},
                            "rawSql": "SELECT COUNT(DISTINCT device_id) FROM local_models WHERE created_at > NOW() - INTERVAL '1 hour'",
                            "format": "table",
                            "refId": "A"
                        }],
                        "options": {
                            "colorMode": "value",
                            "graphMode": "area"
                        }
                    },
                    {
                        "id": 4,
                        "title": "Latest Global Accuracy",
                        "type": "stat",
                        "gridPos": {"h": 4, "w": 6, "x": 18, "y": 0},
                        "targets": [{
                            "datasource": {"uid": self.datasource_uid},
                            "rawSql": "SELECT ROUND(accuracy::numeric, 4) as value FROM federated_models ORDER BY created_at DESC LIMIT 1",
                            "format": "table",
                            "refId": "A"
                        }],
                        "options": {
                            "colorMode": "value",
                            "graphMode": "none"
                        },
                        "fieldConfig": {
                            "defaults": {
                                "unit": "percentunit",
                                "min": 0,
                                "max": 1,
                                "color": {"mode": "thresholds"},
                                "thresholds": {
                                    "mode": "absolute",
                                    "steps": [
                                        {"value": 0, "color": "red"},
                                        {"value": 0.5, "color": "yellow"},
                                        {"value": 0.7, "color": "green"}
                                    ]
                                }
                            }
                        }
                    },
                    # Row 2: Graphs
                    {
                        "id": 5,
                        "title": "Model Training Rate (Per Minute)",
                        "type": "timeseries",
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 4},
                        "targets": [{
                            "datasource": {"uid": self.datasource_uid},
                            "rawSql": """
                                SELECT 
                                    time_bucket('1 minute', created_at) AS time,
                                    COUNT(*) as value
                                FROM local_models
                                WHERE $__timeFilter(created_at)
                                GROUP BY 1
                                ORDER BY 1
                            """,
                            "format": "time_series",
                            "refId": "A"
                        }],
                        "fieldConfig": {
                            "defaults": {
                                "custom": {
                                    "drawStyle": "line",
                                    "lineInterpolation": "smooth",
                                    "fillOpacity": 20,
                                    "showPoints": "never"
                                },
                                "color": {"mode": "palette-classic"}
                            }
                        }
                    },
                    {
                        "id": 6,
                        "title": "Global Model Accuracy Trend",
                        "type": "timeseries",
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 4},
                        "targets": [{
                            "datasource": {"uid": self.datasource_uid},
                            "rawSql": """
                                SELECT 
                                    created_at AS time,
                                    accuracy as value
                                FROM federated_models
                                WHERE $__timeFilter(created_at)
                                ORDER BY created_at
                            """,
                            "format": "time_series",
                            "refId": "A"
                        }],
                        "fieldConfig": {
                            "defaults": {
                                "unit": "percentunit",
                                "min": 0,
                                "max": 1,
                                "custom": {
                                    "drawStyle": "line",
                                    "lineInterpolation": "smooth",
                                    "fillOpacity": 30,
                                    "showPoints": "always",
                                    "pointSize": 5
                                },
                                "color": {"mode": "continuous-GrYlRd"}
                            }
                        }
                    },
                    # Row 3: Tables
                    {
                        "id": 7,
                        "title": "Top Devices by Training Count",
                        "type": "table",
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 12},
                        "targets": [{
                            "datasource": {"uid": self.datasource_uid},
                            "rawSql": """
                                SELECT 
                                    device_id,
                                    COUNT(*) as training_count,
                                    ROUND(AVG(accuracy)::numeric, 4) as avg_accuracy,
                                    SUM(samples_processed) as total_samples
                                FROM local_models
                                WHERE created_at > NOW() - INTERVAL '1 hour'
                                GROUP BY device_id
                                ORDER BY training_count DESC
                                LIMIT 20
                            """,
                            "format": "table",
                            "refId": "A"
                        }],
                        "fieldConfig": {
                            "overrides": [
                                {
                                    "matcher": {"id": "byName", "options": "avg_accuracy"},
                                    "properties": [
                                        {"id": "custom.displayMode", "value": "color-background"},
                                        {"id": "color", "value": {"mode": "continuous-GrYlRd"}}
                                    ]
                                }
                            ]
                        }
                    },
                    {
                        "id": 8,
                        "title": "Recent Federated Models",
                        "type": "table",
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 12},
                        "targets": [{
                            "datasource": {"uid": self.datasource_uid},
                            "rawSql": """
                                SELECT 
                                    global_version,
                                    aggregation_round,
                                    num_devices,
                                    ROUND(accuracy::numeric, 4) as accuracy,
                                    created_at
                                FROM federated_models
                                ORDER BY created_at DESC
                                LIMIT 15
                            """,
                            "format": "table",
                            "refId": "A"
                        }],
                        "fieldConfig": {
                            "overrides": [
                                {
                                    "matcher": {"id": "byName", "options": "accuracy"},
                                    "properties": [
                                        {"id": "custom.displayMode", "value": "gradient-gauge"},
                                        {"id": "min", "value": 0},
                                        {"id": "max", "value": 1}
                                    ]
                                }
                            ]
                        }
                    }
                ]
            },
            "overwrite": True
        }
        
        try:
            response = self.session.post(
                f"{self.base_url}/api/dashboards/db",
                json=dashboard,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code in [200, 201]:
                result = response.json()
                dashboard_url = f"{self.base_url}{result.get('url', '')}"
                logger.info(f"✓ Dashboard created successfully")
                logger.info(f"  URL: {dashboard_url}")
                return True
            else:
                logger.error(f"✗ Failed to create dashboard: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return False
        except Exception as e:
            logger.error(f"✗ Error creating dashboard: {e}")
            return False
    
    def setup(self) -> bool:
        """Run complete setup"""
        logger.info("=" * 70)
        logger.info("GRAFANA AUTOMATED SETUP")
        logger.info("=" * 70)
        
        # Wait for Grafana
        if not self.wait_for_grafana():
            return False
        
        # Create datasource
        if not self.create_datasource():
            return False

        # Dashboards are provisioned from grafana/dashboards/*.json (see
        # grafana/provisioning/dashboards/default.yaml). create_dashboard() is
        # no longer called: it added a ninth, API-created dashboard whose
        # "Latest Global Accuracy" panels showed training accuracy. That
        # dashboard persists in the grafana_data volume, so remove it.
        self.remove_legacy_dashboard()

        logger.info("\n" + "=" * 70)
        logger.info("✓ GRAFANA SETUP COMPLETE")
        logger.info("=" * 70)
        logger.info(f"\nAccess Grafana at: {GRAFANA_URL}")
        logger.info(f"Username: {GRAFANA_USER}")
        logger.info(f"Password: {GRAFANA_PASSWORD}")
        logger.info("\nDashboards: provisioned from grafana/dashboards (folder 'FLEAD')")
        
        return True


def main():
    """Main entry point"""
    configurator = GrafanaConfigurator()
    
    try:
        success = configurator.setup()
        if success:
            logger.info("\n✓ All done! Open Grafana to see your dashboards.")
            return 0
        else:
            logger.error("\n✗ Setup failed. Check the logs above.")
            return 1
    except KeyboardInterrupt:
        logger.info("\nSetup cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"\nUnexpected error: {e}")
        return 1


if __name__ == '__main__':
    exit(main())
