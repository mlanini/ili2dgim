from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class FieldSpec(BaseModel):
    name: str
    cliFlag: str
    type: Literal["string", "number", "boolean"] = "string"
    required: bool = False
    default: Any | None = None
    help: str = ""


class ScriptSpec(BaseModel):
    scriptName: str
    scriptPath: str
    fields: list[FieldSpec] = Field(default_factory=list)


class JobStartRequest(BaseModel):
    scriptName: str
    params: dict[str, Any] = Field(default_factory=dict)
    extraArgs: list[str] = Field(default_factory=list)
    aoiGeoJson: dict[str, Any] | None = None
    aoiParamFlag: str | None = None


class JobSummary(BaseModel):
    id: str
    status: Literal["idle", "running", "paused", "done", "error"]
    scriptName: str | None = None
    command: list[str] = Field(default_factory=list)
    startedAt: float | None = None
    finishedAt: float | None = None
    returnCode: int | None = None
    artifacts: list[str] = Field(default_factory=list)


class JobEvent(BaseModel):
    summary: JobSummary
    newLogs: list[str] = Field(default_factory=list)
