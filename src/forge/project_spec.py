from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


SECTION_KINDS = {
    "models": "model",
    "udfs": "udf",
    "seeds": "seed",
    "volumes": "volume",
    "sources": "source",
}


@dataclass(frozen=True, slots=True)
class AssetRef:
    kind: str
    name: str
    asset_class: str | None = None
    domain: str | None = None
    layer: str | None = None

    @property
    def key(self) -> str:
        parts = [self.kind]
        if self.asset_class:
            parts.append(self.asset_class)
        if self.domain:
            parts.append(self.domain)
        if self.layer:
            parts.append(self.layer)
        parts.append(self.name)
        return ":".join(parts)


@dataclass(slots=True)
class AssetSpec:
    ref: AssetRef
    section: str
    definition: Any
    source_path: Path


@dataclass(slots=True)
class ProjectSpec:
    ddl_path: Path
    forge_config: dict[str, Any] | None = None
    sections: dict[str, dict[str, Any]] = field(
        default_factory=lambda: {
            "models": {},
            "udfs": {},
            "seeds": {},
            "volumes": {},
            "sources": {},
        }
    )
    assets: dict[AssetRef, AssetSpec] = field(default_factory=dict)

    def section_dict(self, section: str) -> dict[str, Any]:
        return dict(self.sections.get(section, {}))

    def to_raw_ddl(self) -> dict[str, dict[str, Any]]:
        return {section: dict(items) for section, items in self.sections.items()}

    @property
    def models(self) -> dict[str, Any]:
        return self.section_dict("models")

    @property
    def udfs(self) -> dict[str, Any]:
        return self.section_dict("udfs")

    @property
    def seeds(self) -> dict[str, Any]:
        return self.section_dict("seeds")

    @property
    def volumes(self) -> dict[str, Any]:
        return self.section_dict("volumes")

    @property
    def sources(self) -> dict[str, Any]:
        return self.section_dict("sources")


def _uses_v1_placements(forge_config: dict | None) -> bool:
    placements = (forge_config or {}).get("placements", {})
    return isinstance(placements.get("families"), dict) and bool(placements["families"])


def _inject_asset_metadata(defn: dict, **metadata: str) -> dict:
    if not isinstance(defn, dict):
        return defn
    updated = dict(defn)
    for key, value in metadata.items():
        if value is not None:
            updated.setdefault(key, value)
    return updated


def _merge_section(
    spec: ProjectSpec,
    section: str,
    raw: dict,
    yml_file: Path,
    inject_layer: str | None = None,
) -> None:
    for name, defn in raw.get(section, {}).items():
        if name in spec.sections[section]:
            raise ValueError(
                f"Duplicate {section.rstrip('s')} '{name}' in {yml_file.name} "
                f"(already defined in an earlier file)"
            )
        if inject_layer and section in ("models", "udfs", "volumes") and isinstance(defn, dict):
            defn = dict(defn)
            defn.setdefault("layer", inject_layer)

        spec.sections[section][name] = defn
        ref = AssetRef(
            kind=SECTION_KINDS[section],
            name=name,
            asset_class=defn.get("class") if isinstance(defn, dict) else None,
            domain=defn.get("domain") if isinstance(defn, dict) else None,
            layer=defn.get("layer") if isinstance(defn, dict) else None,
        )
        spec.assets[ref] = AssetSpec(
            ref=ref,
            section=section,
            definition=defn,
            source_path=yml_file,
        )


def _load_project_spec_legacy(ddl_path: Path, forge_config: dict | None = None) -> ProjectSpec:
    spec = ProjectSpec(ddl_path=ddl_path, forge_config=forge_config)
    allowed_layers = set(forge_config.get("schemas", [])) | set(forge_config.get("catalogs", [])) if forge_config else set()
    allowed_sections = set(SECTION_KINDS)

    root_ymls = sorted(ddl_path.glob("*.yml"))
    if root_ymls:
        names = ", ".join(p.name for p in root_ymls)
        raise ValueError(
            "Canonical DDL does not allow YAML files directly under dbt/ddl/. "
            f"Move these into dbt/ddl/<layer>/<section>/: {names}"
        )

    layer_dirs = [d for d in sorted(ddl_path.iterdir()) if d.is_dir()]
    if not layer_dirs:
        raise ValueError(
            "No layer folders found under dbt/ddl/. Expected dbt/ddl/<layer>/<section>/*.yml"
        )

    for layer_dir in layer_dirs:
        if allowed_layers and layer_dir.name not in allowed_layers:
            raise ValueError(
                f"Folder '{layer_dir.name}' in {ddl_path} is not a valid layer. "
                f"Allowed layers (from forge.yml schemas + catalogs): {sorted(allowed_layers)}"
            )

        section_dirs = [d for d in sorted(layer_dir.iterdir()) if d.is_dir()]
        stray_ymls = sorted(layer_dir.glob("*.yml"))
        if stray_ymls:
            names = ", ".join(p.name for p in stray_ymls)
            raise ValueError(
                f"Layer '{layer_dir.name}' contains YAML files outside the canonical section folders: {names}"
            )
        if not section_dirs:
            continue

        for section_dir in section_dirs:
            if section_dir.name not in allowed_sections:
                raise ValueError(
                    f"Invalid DDL section '{section_dir.name}' under layer '{layer_dir.name}'. "
                    f"Allowed sections: {sorted(allowed_sections)}"
                )

            for yml_file in sorted(section_dir.rglob("*.yml")):
                raw = yaml.safe_load(yml_file.read_text()) or {}
                if section_dir.name not in raw:
                    raise ValueError(
                        f"{yml_file} is under '{section_dir.name}/' but does not define a top-level '{section_dir.name}:' block"
                    )
                unexpected = set(raw) - {section_dir.name}
                if unexpected:
                    raise ValueError(
                        f"{yml_file} is under '{section_dir.name}/' but defines unrelated sections: {sorted(unexpected)}"
                    )
                _merge_section(
                    spec,
                    section_dir.name,
                    raw,
                    yml_file,
                    inject_layer=layer_dir.name,
                )

    return spec


def _load_project_spec_v1(ddl_path: Path, forge_config: dict | None = None) -> ProjectSpec:
    spec = ProjectSpec(ddl_path=ddl_path, forge_config=forge_config)
    allowed_sections = set(SECTION_KINDS)
    allowed_classes = {"domain", "shared"}
    allowed_domain_layers = {"bronze", "silver", "gold"}
    allowed_shared_layers = {"meta", "operations"}

    root_ymls = sorted(ddl_path.glob("*.yml"))
    if root_ymls:
        names = ", ".join(p.name for p in root_ymls)
        raise ValueError(
            "V1 DDL does not allow YAML files directly under dbt/ddl/. "
            f"Move these into dbt/ddl/domain/... or dbt/ddl/shared/...: {names}"
        )

    class_dirs = [d for d in sorted(ddl_path.iterdir()) if d.is_dir()]
    if not class_dirs:
        raise ValueError(
            "No DDL class folders found under dbt/ddl/. Expected dbt/ddl/domain/... or dbt/ddl/shared/..."
        )

    for class_dir in class_dirs:
        if class_dir.name not in allowed_classes:
            raise ValueError(
                f"Invalid top-level DDL folder '{class_dir.name}'. Allowed folders: {sorted(allowed_classes)}"
            )

        stray_ymls = sorted(class_dir.glob("*.yml"))
        if stray_ymls:
            names = ", ".join(p.name for p in stray_ymls)
            raise ValueError(
                f"Folder '{class_dir.name}' contains YAML files outside the canonical v1 tree: {names}"
            )

        if class_dir.name == "domain":
            domain_dirs = [d for d in sorted(class_dir.iterdir()) if d.is_dir()]
            if not domain_dirs:
                continue

            for domain_dir in domain_dirs:
                stray_domain_ymls = sorted(domain_dir.glob("*.yml"))
                if stray_domain_ymls:
                    names = ", ".join(p.name for p in stray_domain_ymls)
                    raise ValueError(
                        f"Domain '{domain_dir.name}' contains YAML files outside its layer folders: {names}"
                    )

                layer_dirs = [d for d in sorted(domain_dir.iterdir()) if d.is_dir()]
                if not layer_dirs:
                    continue

                for layer_dir in layer_dirs:
                    if layer_dir.name not in allowed_domain_layers:
                        raise ValueError(
                            f"Invalid domain layer '{layer_dir.name}' under '{domain_dir.name}'. "
                            f"Allowed layers: {sorted(allowed_domain_layers)}"
                        )
                    stray_layer_ymls = sorted(layer_dir.glob("*.yml"))
                    if stray_layer_ymls:
                        names = ", ".join(p.name for p in stray_layer_ymls)
                        raise ValueError(
                            f"Layer '{domain_dir.name}/{layer_dir.name}' contains YAML files outside section folders: {names}"
                        )

                    for section_dir in [d for d in sorted(layer_dir.iterdir()) if d.is_dir()]:
                        if section_dir.name not in allowed_sections:
                            raise ValueError(
                                f"Invalid DDL section '{section_dir.name}' under domain layer '{domain_dir.name}/{layer_dir.name}'. "
                                f"Allowed sections: {sorted(allowed_sections)}"
                            )
                        for yml_file in sorted(section_dir.rglob("*.yml")):
                            raw = yaml.safe_load(yml_file.read_text()) or {}
                            if section_dir.name not in raw:
                                raise ValueError(
                                    f"{yml_file} is under '{section_dir.name}/' but does not define a top-level '{section_dir.name}:' block"
                                )
                            unexpected = set(raw) - {section_dir.name}
                            if unexpected:
                                raise ValueError(
                                    f"{yml_file} is under '{section_dir.name}/' but defines unrelated sections: {sorted(unexpected)}"
                                )
                            scoped_raw = {
                                section_dir.name: {
                                    name: _inject_asset_metadata(
                                        defn,
                                        **{
                                            "class": "domain",
                                            "domain": domain_dir.name,
                                            "layer": layer_dir.name,
                                        },
                                    )
                                    for name, defn in raw.get(section_dir.name, {}).items()
                                }
                            }
                            _merge_section(spec, section_dir.name, scoped_raw, yml_file)

        if class_dir.name == "shared":
            layer_dirs = [d for d in sorted(class_dir.iterdir()) if d.is_dir()]
            if not layer_dirs:
                continue

            for layer_dir in layer_dirs:
                if layer_dir.name not in allowed_shared_layers:
                    raise ValueError(
                        f"Invalid shared layer '{layer_dir.name}'. Allowed layers: {sorted(allowed_shared_layers)}"
                    )
                stray_layer_ymls = sorted(layer_dir.glob("*.yml"))
                if stray_layer_ymls:
                    names = ", ".join(p.name for p in stray_layer_ymls)
                    raise ValueError(
                        f"Shared layer '{layer_dir.name}' contains YAML files outside section folders: {names}"
                    )

                for section_dir in [d for d in sorted(layer_dir.iterdir()) if d.is_dir()]:
                    if section_dir.name not in allowed_sections:
                        raise ValueError(
                            f"Invalid DDL section '{section_dir.name}' under shared layer '{layer_dir.name}'. "
                            f"Allowed sections: {sorted(allowed_sections)}"
                        )
                    for yml_file in sorted(section_dir.rglob("*.yml")):
                        raw = yaml.safe_load(yml_file.read_text()) or {}
                        if section_dir.name not in raw:
                            raise ValueError(
                                f"{yml_file} is under '{section_dir.name}/' but does not define a top-level '{section_dir.name}:' block"
                            )
                        unexpected = set(raw) - {section_dir.name}
                        if unexpected:
                            raise ValueError(
                                f"{yml_file} is under '{section_dir.name}/' but defines unrelated sections: {sorted(unexpected)}"
                            )
                        scoped_raw = {
                            section_dir.name: {
                                name: _inject_asset_metadata(
                                    defn,
                                    **{
                                        "class": "shared",
                                        "layer": layer_dir.name,
                                    },
                                )
                                for name, defn in raw.get(section_dir.name, {}).items()
                            }
                        }
                        _merge_section(spec, section_dir.name, scoped_raw, yml_file)

    return spec


def load_project_spec(ddl_path: Path, forge_config: dict[str, Any] | None = None) -> ProjectSpec:
    if not ddl_path.exists():
        raise FileNotFoundError(f"DDL path not found: {ddl_path}")
    if not ddl_path.is_dir():
        raise ValueError(
            f"DDL path must be a directory tree rooted at dbt/ddl/. Got: {ddl_path}"
        )

    if _uses_v1_placements(forge_config):
        return _load_project_spec_v1(ddl_path, forge_config=forge_config)
    return _load_project_spec_legacy(ddl_path, forge_config=forge_config)