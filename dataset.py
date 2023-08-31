from typing import Dict, List, Optional

import click
import yaml
import logging
from datahub.emitter.mce_builder import (make_data_platform_urn,
                                         make_dataset_urn)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.metadata.schema_classes import (DatasetPropertiesClass,
                                             OtherSchemaClass,
                                             SchemaMetadataClass,
                                             SubTypesClass, UpstreamClass)
from datahub.specific.dataset import DatasetPatchBuilder
from pydantic import BaseModel, Field, validator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SchemaSpecification(BaseModel):
    file: Optional[str]

    @validator("file")
    def file_must_be_avsc(v):
        if v and not v.endswith(".avsc"):
            raise ValueError("file must be a .avsc file")
        return v


class Dataset(BaseModel):
    id: Optional[str]
    platform: Optional[str]
    env: str = "PROD"
    urn: Optional[str]
    description: Optional[str]
    name: Optional[str]
    schema_field: Optional[SchemaSpecification] = Field(alias="schema")
    downstreams: Optional[List[str]]
    terms: Optional[list]
    properties: Optional[Dict[str, str]]
    subtype: Optional[str]
    subtypes: Optional[List[str]]

    @validator("urn", pre=True, always=True)
    def urn_must_be_present(cls, v, values):
        if not v:
            assert "id" in values, "id must be present if urn is not"
            assert "platform" in values, "platform must be present if urn is not"
            assert "env" in values, "env must be present if urn is not"
            return make_dataset_urn(values["platform"], values["id"], values["env"])
        return v

    @validator("name", pre=True, always=True)
    def name_filled_with_id_if_not_present(cls, v, values):
        if not v:
            assert "id" in values, "id must be present if name is not"
            return values["id"]
        return v

    @validator("platform")
    def platform_must_not_be_urn(v):
        if v.startswith("urn:li:dataPlatform:"):
            return v[len("urn:li:dataPlatform:") :]
        return v


@click.group()
def dataset():
    pass


@dataset.command()
@click.option("--file", required=True, help="path to dataset.yaml file")
def create(file: str):
    emitter: DataHubGraph
    with get_default_graph() as emitter:
        with open(file, "r") as fp:
            datasets: List[dict] = yaml.safe_load(fp)
            for dataset in datasets:
                dataset = Dataset.parse_obj(dataset)
                mcp = MetadataChangeProposalWrapper(
                    entityUrn=dataset.urn,
                    aspect=DatasetPropertiesClass(
                        description=dataset.description,
                        name=dataset.name,
                        customProperties=dataset.properties,
                    ),
                )
                emitter.emit_mcp(mcp)
                with open(dataset.schema_field.file, "r") as fp:
                    schema_string = fp.read()
                    schema_metadata = SchemaMetadataClass(
                        schemaName="test",
                        platform=make_data_platform_urn(dataset.platform),
                        version=0,
                        hash="",
                        platformSchema=OtherSchemaClass(rawSchema=schema_string),
                        fields=avro_schema_to_mce_fields(schema_string),
                    )
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=dataset.urn, aspect=schema_metadata
                    )
                    emitter.emit_mcp(mcp)

                if dataset.subtype or dataset.subtypes:
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=dataset.urn,
                        aspect=SubTypesClass(
                            typeNames=[
                                s
                                for s in [dataset.subtype] + (dataset.subtypes or [])
                                if s
                            ]
                        ),
                    )
                    emitter.emit_mcp(mcp)

                if dataset.downstreams:
                    for downstream in dataset.downstreams:
                        patch_builder = DatasetPatchBuilder(downstream)
                        patch_builder.add_upstream_lineage(
                            UpstreamClass(
                                dataset=dataset.urn,
                                type="COPY",
                            )
                        )
                        for mcp in patch_builder.build():
                            emitter.emit(mcp)
                logger.info(f"Created dataset {dataset.urn}")

if __name__ == "__main__":
    dataset()
