# Copyright 2025-2026 Pricefx
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections.abc import Iterator
from io import TextIOBase
import json
from typing import Any, Optional
from urllib.parse import ParseResult

import pandas as pd

from pyfxlib.lowlevel.connection import ConnectionAsync, ConnectionRemote
from pyfxlib.lowlevel.session import PfxAuthUserPass, PfxSession


class _IntegrationRemote:
    def __init__(
        self, session: PfxSession, auth: PfxAuthUserPass, pfx_base_url: ParseResult
    ) -> None:
        self._session = session
        self._pfx_base_url = pfx_base_url
        self._auth = auth
        self._conn = ConnectionRemote(
            pfx_base_url._replace(path="/pricefx/system").geturl(), session
        )

    def connection(self) -> ConnectionAsync:
        return self._conn

    def endpoint_url(self, path_suffix: str = "") -> str:
        return self._pfx_base_url._replace(
            path=f"/pricefx/system/{path_suffix.lstrip('/')}"
        ).geturl()

    async def jwt(self) -> str:
        # initialize auth if not already done
        await self._auth.before_request(self._session)
        return self._auth.pfxtoken

    def jwt_sync(self) -> str:
        if self._auth.pfxtoken is None:
            raise RuntimeError("Not authenticated yet; call jwt() in an async context first")
        return self._auth.pfxtoken

    async def trigger_job(self, mo: dict[str, Any]) -> None:
        await self._session.post(
            self.endpoint_url(f"remoteintegrationtestmanager/createJobTriggerTask/{mo['typedId']}"),
            json={
                "data": {
                    "modelId": mo["typedId"].split(".")[0],
                    "modelName": mo["uniqueName"],
                    "parameters": "someParameters",
                    "imageName": "imageName",
                    "imageTag": "imageTag",
                }
            },
        )

    async def jobs(self, mo_typedid: str) -> list[dict[str, Any]]:
        response = await self._session.post(
            self._pfx_base_url._replace(
                path="/pricefx/system/admin.fetchjst",
            ).geturl(),
            json={"data": {"targetObject": mo_typedid}},
        )
        return response.json()["response"]["data"]

    async def job(self, mo_typedid: str) -> dict[str, Any]:
        jobs = await self.jobs(mo_typedid)
        if len(jobs) != 1:
            raise Exception(f"Should have only one job for {mo_typedid}, got {len(jobs)}")
        return await self.connection().get_object(f"{jobs[0]['id']}.JST")

    async def new_model_object(
        self,
        unique_name: str,
        model_class: Optional[dict[str, Any]] = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        mc = model_class if model_class is not None else await self.new_model_class()
        return (
            mc,
            await self._conn.add_object(
                "MO",
                {
                    "uniqueName": unique_name,
                    "modelClassUN": mc["uniqueName"],
                    "state": {},
                    "workflowStatus": "DRAFT",
                },
            ),
        )

    async def new_model_class(self, unique_name: str = "aModelClass") -> dict[str, Any]:
        return await self._conn.add_object(
            "MC",
            {
                "uniqueName": unique_name,
                "definition": {
                    "evaluations": [],
                    "calculations": [],
                    "steps": [],
                },
                "workflowFormulaName": None,
            },
        )

    async def new_empty_datamart(self, name: str, fields_spec: Optional[list[dict]]):
        response = await self._session.post(
            self.endpoint_url("datamart.newfc/DM"),
            json={"data": {"uniqueName": name, "label": name}},
        )
        entry = response.json()["response"]["data"][0]
        await self._session.post(
            self.endpoint_url("datamart.updatefc/DM"),
            json={
                "data": {
                    "typedId": entry["typedId"],
                    "version": entry["version"],
                    "uniqueName": name,
                    "label": name,
                    "source": "",
                    "fields": fields_spec,
                    "deployed": True,
                }
            },
        )


def _calculation_results_as_dict(
    calc_results_as_json: str,
) -> dict[str, dict[str, Any]]:
    return {
        calc_res["resultName"]: calc_res["result"] for calc_res in json.loads(calc_results_as_json)
    }


class _StringIteratorIO(TextIOBase):
    def __init__(self, iter: Iterator[bytes]):
        self._iterator = iter
        self._buffer = ""

    def readable(self):
        return True

    def read(self, n: Optional[int] = None):
        while not self._buffer:
            try:
                self._buffer = next(self._iterator).decode("utf-8")
            except StopIteration:
                break
        result = self._buffer[:n]
        self._buffer = self._buffer[len(result) :]
        return result


def _csv_stream_to_dataframe(csv_data: Iterator[bytes]) -> pd.DataFrame:
    return pd.read_csv(_StringIteratorIO(csv_data))
