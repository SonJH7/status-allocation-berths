# db.py
# SQLAlchemy models & helpers (SQLite by default)
import os
import uuid
from datetime import datetime
from typing import Dict, Iterable, List

import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Float, text
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

DB_URL = os.environ.get("DATABASE_URL", "sqlite:///berth.db")
engine = create_engine(DB_URL, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)
Base = declarative_base()

class Berth(Base):
    __tablename__ = "berths"
    id = Column(Integer, primary_key=True)
    code = Column(String, unique=True, nullable=False)
    meter_start = Column(Integer, default=0)
    meter_end = Column(Integer, default=400)

class Vessel(Base):
    __tablename__ = "vessels"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    loa_m = Column(Float)  # Length Overall
    imo = Column(String)
    mmsi = Column(String)

class ScheduleVersion(Base):
    __tablename__ = "schedule_versions"
    id = Column(String, primary_key=True)  # uuid
    source = Column(String, nullable=False)
    label = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

    assignments = relationship("Assignment", back_populates="version", cascade="all, delete-orphan")

class Assignment(Base):
    __tablename__ = "assignments"
    id = Column(String, primary_key=True)  # uuid
    version_id = Column(String, ForeignKey("schedule_versions.id"))
    vessel_id = Column(Integer, ForeignKey("vessels.id"))
    berth_id = Column(Integer, ForeignKey("berths.id"))
    eta = Column(DateTime, nullable=False)
    etd = Column(DateTime, nullable=False)
    start_meter = Column(Float)  # optional spatial start
    notes = Column(String)

    version = relationship("ScheduleVersion", back_populates="assignments")
    vessel = relationship("Vessel")
    berth = relationship("Berth")

def init_db():
    Base.metadata.create_all(engine)
    return engine, Base

def upsert_reference_data(session):
    # Seed berths from CSV if table empty
    if session.query(Berth).count() == 0:
        path = os.path.join(os.path.dirname(__file__), "data", "berths.csv")
        if os.path.exists(path):
            df = pd.read_csv(path)
            for _, r in df.iterrows():
                session.add(Berth(code=str(r["code"]), meter_start=int(r["meter_start"]), meter_end=int(r["meter_end"])))
        else:
            # default placeholder
            for b in ["B1","B2","B3","B4","B5"]:
                session.add(Berth(code=b, meter_start=0, meter_end=400))
        session.commit()

    # Seed LOA (vessels)
    if session.query(Vessel).count() == 0:
        path = os.path.join(os.path.dirname(__file__), "data", "vessels_loa.csv")
        if os.path.exists(path):
            df = pd.read_csv(path)
            for _, r in df.iterrows():
                session.add(Vessel(name=str(r["name"]), loa_m=float(r["loa_m"])))
            session.commit()

def _get_or_create_vessel(session, name: str) -> Vessel:
    v = session.query(Vessel).filter(Vessel.name==name).one_or_none()
    if v is None:
        v = Vessel(name=name)
        session.add(v)
        session.commit()
    return v

def _get_berth(session, code: str) -> Berth:
    b = session.query(Berth).filter(Berth.code==code).one_or_none()
    if b is None:
        b = Berth(code=code, meter_start=0, meter_end=400)
        session.add(b)
        session.commit()
    return b


def get_vessel_loa_map(session, vessel_names: Iterable[str]) -> Dict[str, float]:
    """요청된 선박명에 대한 LOA 정보를 반환한다."""

    names = {str(name).strip() for name in vessel_names if str(name).strip()}
    if not names:
        return {}

    rows = (
        session.query(Vessel.name, Vessel.loa_m)
        .filter(Vessel.name.in_(names))
        .all()
    )
    return {name: loa for name, loa in rows if loa is not None}


def set_vessels_loa(session, mapping: Dict[str, float]) -> int:
    """선박 LOA 값을 일괄 업데이트하고 변경 건수를 반환한다."""

    changed = 0
    for raw_name, raw_loa in mapping.items():
        if raw_loa is None:
            continue
        try:
            loa_val = float(raw_loa)
        except (TypeError, ValueError):
            continue

        name = str(raw_name).strip()
        if not name:
            continue

        vessel = session.query(Vessel).filter(Vessel.name == name).one_or_none()
        if vessel is None:
            vessel = Vessel(name=name, loa_m=loa_val)
            session.add(vessel)
            changed += 1
        elif vessel.loa_m != loa_val:
            vessel.loa_m = loa_val
            changed += 1

    if changed:
        session.commit()
    return changed

def create_version_with_assignments(session, df: pd.DataFrame, source="user-edit", label=""):
    vid = str(uuid.uuid4())
    ver = ScheduleVersion(id=vid, source=source, label=label)
    session.add(ver)
    session.commit()

    # Expected columns: vessel, berth, eta, etd, [loa_m, start_meter]
    for _, r in df.iterrows():
        vessel = _get_or_create_vessel(session, str(r["vessel"]))
        if "loa_m" in df.columns and pd.notna(r.get("loa_m", None)) and not vessel.loa_m:
            vessel.loa_m = float(r["loa_m"])
            session.add(vessel)

        berth = _get_berth(session, str(r["berth"]))
        a = Assignment(
            id=str(uuid.uuid4()),
            version_id=vid,
            vessel_id=vessel.id,
            berth_id=berth.id,
            eta=pd.to_datetime(r["eta"]),
            etd=pd.to_datetime(r["etd"]),
            start_meter=float(r["start_meter"]) if "start_meter" in df.columns and pd.notna(r.get("start_meter", None)) else None,
        )
        session.add(a)

    session.commit()
    return vid

def list_versions(session) -> List[Dict]:
    out = []
    for v in session.query(ScheduleVersion).order_by(ScheduleVersion.created_at.desc()).all():
        out.append({
            "id": v.id,
            "source": v.source,
            "label": v.label,
            "created_at": v.created_at,
            "count": len(v.assignments),
        })
    return out

def load_assignments_df(session, version_id: str) -> pd.DataFrame:
    q = session.query(Assignment, Vessel, Berth).join(Vessel, Assignment.vessel_id==Vessel.id).join(Berth, Assignment.berth_id==Berth.id).filter(Assignment.version_id==version_id)
    rows = []
    for a, v, b in q.all():
        rows.append({
            "vessel": v.name,
            "berth": b.code,
            "eta": a.eta,
            "etd": a.etd,
            "loa_m": v.loa_m,
            "start_meter": a.start_meter,
        })
    return pd.DataFrame(rows)


def delete_versions(session, version_ids: Iterable[str] | None = None) -> int:
    """지정된 버전을 삭제하고 삭제된 개수를 반환한다."""

    query = session.query(ScheduleVersion)
    if version_ids is not None:
        ids = {vid for vid in version_ids if vid}
        if not ids:
            return 0
        query = query.filter(ScheduleVersion.id.in_(ids))

    versions = query.all()
    if not versions:
        return 0

    count = len(versions)
    for version in versions:
        session.delete(version)
    session.commit()
    return count


def delete_all_versions(session) -> int:
    """모든 선석 배정 버전을 삭제한다."""

    return delete_versions(session, None)
