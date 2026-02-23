"""Nuclear waste inventory and compliance tracking."""
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import argparse
import csv
import json
import math


@dataclass
class WasteContainer:
    """Represents a nuclear waste container."""
    id: str
    label: str
    waste_type: str  # low_level, intermediate, high_level, transuranic, exempt
    isotopes: str  # JSON list of isotopes
    activity_bq: float  # Becquerels
    volume_l: float
    mass_kg: float
    location: str
    storage_class: str
    created_at: str
    decay_date: str  # Estimated safe decay date
    status: str  # "active", "decayed", "transferred"


@dataclass
class TransferRecord:
    """Represents a transfer of waste container."""
    container_id: str
    from_location: str
    to_location: str
    transferred_by: str
    ts: str
    manifested: bool


class NuclearWasteTracker:
    """Nuclear waste tracking and compliance system."""
    
    # Half-lives in years
    HALF_LIVES = {
        "Cs-137": 30.17,
        "Co-60": 5.27,
        "Sr-90": 28.8,
        "H-3": 12.32,
        "C-14": 5730,
        "Pu-239": 24100
    }
    
    # Storage class limits (activity in Bq)
    STORAGE_LIMITS = {
        "low_level": 1e6,      # 1 MBq
        "intermediate": 1e9,   # 1 GBq
        "high_level": 1e12,    # 1 TBq
        "transuranic": 1e6,    # 1 MBq
        "exempt": 1e3           # 1 kBq
    }
    
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = os.path.expanduser("~/.blackroad/waste.db")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize database tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS containers (
                id TEXT PRIMARY KEY,
                label TEXT,
                waste_type TEXT,
                isotopes TEXT,
                activity_bq REAL,
                volume_l REAL,
                mass_kg REAL,
                location TEXT,
                storage_class TEXT,
                created_at TEXT,
                decay_date TEXT,
                status TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                container_id TEXT,
                from_location TEXT,
                to_location TEXT,
                transferred_by TEXT,
                ts TEXT,
                manifested BOOLEAN,
                FOREIGN KEY(container_id) REFERENCES containers(id)
            )
        """)
        
        conn.commit()
        conn.close()
    
    def _get_conn(self):
        """Get database connection."""
        return sqlite3.connect(self.db_path)
    
    def register_container(self, label: str, waste_type: str, isotopes: List[str],
                          activity_bq: float, volume_l: float, mass_kg: float,
                          location: str, storage_class: str) -> str:
        """Register a new waste container."""
        import uuid
        container_id = str(uuid.uuid4())[:8]
        created_at = datetime.now().isoformat()
        
        # Estimate decay date
        decay_date = self._calc_decay_date(isotopes, activity_bq)
        
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO containers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (container_id, label, waste_type, json.dumps(isotopes),
              activity_bq, volume_l, mass_kg, location, storage_class,
              created_at, decay_date, "active"))
        conn.commit()
        conn.close()
        
        return container_id
    
    def _calc_decay_date(self, isotopes: List[str], initial_activity: float) -> str:
        """Calculate when container reaches safe disposal threshold (1 kBq)."""
        SAFE_THRESHOLD = 1000  # 1 kBq
        
        if not isotopes:
            return (datetime.now() + timedelta(days=365)).isoformat()
        
        # Use longest half-life
        max_half_life = max(self.HALF_LIVES.get(iso, 1) for iso in isotopes)
        
        # Calculate time to reach threshold
        if initial_activity <= SAFE_THRESHOLD:
            return datetime.now().isoformat()
        
        # t = t_half * log(A0/A) / log(2)
        years_needed = max_half_life * math.log(initial_activity / SAFE_THRESHOLD) / math.log(2)
        decay_date = datetime.now() + timedelta(days=years_needed * 365)
        
        return decay_date.isoformat()
    
    def transfer(self, container_id: str, to_location: str, transferred_by: str) -> bool:
        """Create transfer record."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT location FROM containers WHERE id = ?", (container_id,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return False
        
        from_location = row[0]
        now = datetime.now().isoformat()
        
        # Create transfer record
        cursor.execute("""
            INSERT INTO transfers (container_id, from_location, to_location, transferred_by, ts, manifested)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (container_id, from_location, to_location, transferred_by, now, False))
        
        # Update container location
        cursor.execute("UPDATE containers SET location = ? WHERE id = ?",
                      (to_location, container_id))
        
        conn.commit()
        conn.close()
        return True
    
    def decay_correct(self, container_id: str) -> float:
        """Recalculate current activity using decay constant."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT isotopes, activity_bq, created_at FROM containers WHERE id = ?",
                      (container_id,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return 0
        
        isotopes_str, initial_activity, created_at = row
        isotopes = json.loads(isotopes_str)
        
        # Use average half-life
        avg_half_life = sum(self.HALF_LIVES.get(iso, 1) for iso in isotopes) / len(isotopes) if isotopes else 1
        
        created = datetime.fromisoformat(created_at)
        elapsed_years = (datetime.now() - created).days / 365.25
        
        # A = A0 * (1/2)^(t/t_half)
        current_activity = initial_activity * (0.5 ** (elapsed_years / avg_half_life))
        
        return current_activity
    
    def get_inventory(self, location: str = None, waste_type: str = None) -> List[WasteContainer]:
        """Get filtered container inventory."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        query = "SELECT * FROM containers WHERE status = 'active'"
        params = []
        
        if location:
            query += " AND location = ?"
            params.append(location)
        if waste_type:
            query += " AND waste_type = ?"
            params.append(waste_type)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        containers = []
        for row in rows:
            containers.append(WasteContainer(
                id=row[0], label=row[1], waste_type=row[2], isotopes=row[3],
                activity_bq=row[4], volume_l=row[5], mass_kg=row[6],
                location=row[7], storage_class=row[8], created_at=row[9],
                decay_date=row[10], status=row[11]
            ))
        
        return containers
    
    def compliance_check(self) -> Dict:
        """Check storage compliance."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        issues = {
            "storage_class_violations": [],
            "expired_containers": [],
            "missing_manifests": []
        }
        
        # Check storage class limits
        cursor.execute("SELECT id, storage_class, activity_bq FROM containers WHERE status = 'active'")
        containers = cursor.fetchall()
        
        for container_id, storage_class, activity in containers:
            limit = self.STORAGE_LIMITS.get(storage_class, 1e6)
            if activity > limit:
                issues["storage_class_violations"].append({
                    "container_id": container_id,
                    "activity_bq": activity,
                    "limit_bq": limit
                })
        
        # Check expired containers
        now = datetime.now().isoformat()
        cursor.execute("SELECT id FROM containers WHERE decay_date < ? AND status = 'active'",
                      (now,))
        for (cid,) in cursor.fetchall():
            issues["expired_containers"].append(cid)
        
        # Check missing manifests
        cursor.execute("""
            SELECT id FROM containers 
            WHERE id IN (
                SELECT container_id FROM transfers WHERE manifested = 0
            ) AND status = 'active'
        """)
        for (cid,) in cursor.fetchall():
            issues["missing_manifests"].append(cid)
        
        conn.close()
        return issues
    
    def generate_manifest(self, transfer_id: int) -> str:
        """Generate regulatory manifest document."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT container_id, from_location, to_location, transferred_by, ts
            FROM transfers WHERE id = ?
        """, (transfer_id,))
        
        transfer = cursor.fetchone()
        if not transfer:
            conn.close()
            return ""
        
        container_id, from_loc, to_loc, transferred_by, ts = transfer
        
        cursor.execute("""
            SELECT label, waste_type, isotopes, activity_bq, volume_l, mass_kg
            FROM containers WHERE id = ?
        """, (container_id,))
        
        container = cursor.fetchone()
        conn.close()
        
        if not container:
            return ""
        
        manifest = f"""
NUCLEAR WASTE TRANSFER MANIFEST
Generated: {datetime.now().isoformat()}

CONTAINER INFORMATION:
  ID: {container_id}
  Label: {container[0]}
  Waste Type: {container[1]}
  Isotopes: {container[2]}
  Activity: {container[3]:.2e} Bq
  Volume: {container[4]} L
  Mass: {container[5]} kg

TRANSFER DETAILS:
  From: {from_loc}
  To: {to_loc}
  Transferred By: {transferred_by}
  Date: {ts}
  Transfer ID: {transfer_id}

CERTIFICATION:
This document certifies proper handling and transfer of radioactive material
in compliance with regulatory requirements.
"""
        return manifest
    
    def total_activity(self, location: str = None) -> float:
        """Get total activity at location or globally."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        if location:
            cursor.execute("""
                SELECT SUM(activity_bq) FROM containers
                WHERE location = ? AND status = 'active'
            """, (location,))
        else:
            cursor.execute("""
                SELECT SUM(activity_bq) FROM containers
                WHERE status = 'active'
            """)
        
        result = cursor.fetchone()[0]
        conn.close()
        
        return result if result else 0.0
    
    def export_csv(self, output_path: str) -> bool:
        """Export inventory to CSV."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM containers ORDER BY created_at")
        containers = cursor.fetchall()
        conn.close()
        
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["ID", "Label", "Type", "Isotopes", "Activity_Bq", 
                            "Volume_L", "Mass_kg", "Location", "Class", 
                            "Created", "DecayDate", "Status"])
            for row in containers:
                writer.writerow(row)
        
        return True
    
    def decay_schedule(self) -> List[Dict]:
        """Get schedule of when containers reach safe threshold."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, label, decay_date, activity_bq FROM containers
            WHERE status = 'active' ORDER BY decay_date
        """)
        
        containers = cursor.fetchall()
        conn.close()
        
        schedule = []
        for container_id, label, decay_date, activity in containers:
            schedule.append({
                "container_id": container_id,
                "label": label,
                "current_activity_bq": activity,
                "safe_decay_date": decay_date,
                "days_until_safe": (datetime.fromisoformat(decay_date) - datetime.now()).days
            })
        
        return schedule


def cli():
    """Command-line interface."""
    parser = argparse.ArgumentParser(description="Nuclear Waste Tracker")
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # inventory command
    inv_parser = subparsers.add_parser("inventory", help="Show inventory")
    inv_parser.add_argument("--location", help="Filter by location")
    inv_parser.add_argument("--type", help="Filter by waste type")
    
    # compliance command
    comp_parser = subparsers.add_parser("compliance", help="Check compliance")
    
    # decay-schedule command
    decay_parser = subparsers.add_parser("decay-schedule", help="Show decay schedule")
    
    args = parser.parse_args()
    tracker = NuclearWasteTracker()
    
    if args.command == "inventory":
        containers = tracker.get_inventory(location=args.location, waste_type=getattr(args, 'type', None))
        for container in containers:
            print(f"{container.id} | {container.label} | {container.waste_type} | "
                  f"{container.activity_bq:.2e} Bq | {container.location}")
    
    elif args.command == "compliance":
        issues = tracker.compliance_check()
        print(f"Storage Class Violations: {len(issues['storage_class_violations'])}")
        print(f"Expired Containers: {len(issues['expired_containers'])}")
        print(f"Missing Manifests: {len(issues['missing_manifests'])}")
        if issues['storage_class_violations']:
            for v in issues['storage_class_violations']:
                print(f"  - {v['container_id']}: {v['activity_bq']:.2e} Bq (limit: {v['limit_bq']:.2e})")
    
    elif args.command == "decay-schedule":
        schedule = tracker.decay_schedule()
        for item in schedule:
            print(f"{item['container_id']} | {item['label']} | Safe in {item['days_until_safe']} days")


if __name__ == "__main__":
    cli()
