import json
from io import TextIOBase
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import ParseResult

import pandas as pd

from pyfx2.lowlevel.connection import Connection, ConnectionRemote
from pyfx2.lowlevel.session import PfxAuthUserPass, PfxSession


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

    def connection(self) -> Connection:
        return self._conn

    def endpoint_url(self, path_suffix: str = "") -> str:
        return self._pfx_base_url._replace(
            path=f"/pricefx/system/{path_suffix.lstrip('/')}"
        ).geturl()

    def jwt(self) -> str:
        # initialize auth if not already done
        self._auth.before_request(self._session)
        return self._auth.pfxtoken

    def trigger_job(self, mo: Dict[str, Any]) -> None:
        self._session.post(
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

    def jobs(self, mo_typedid: str) -> List[Dict[str, Any]]:
        return self._session.post(
            self._pfx_base_url._replace(
                path="/pricefx/system/admin.fetchjst",
            ).geturl(),
            json={"data": {"targetObject": mo_typedid}},
        ).json()["response"]["data"]

    def job(self, mo_typedid: str) -> Dict[str, Any]:
        jobs = self.jobs(mo_typedid)
        if len(jobs) != 1:
            raise Exception(f"Should have only one job for {mo_typedid}, got {len(jobs)}")
        return self.connection().get_object(f"{jobs[0]['id']}.JST")

    def new_model_object(
        self,
        unique_name: str,
        model_class: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        mc = model_class if model_class is not None else self.new_model_class()
        return (
            mc,
            self._conn.add_object(
                "MO",
                {
                    "uniqueName": unique_name,
                    "modelClassUN": mc["uniqueName"],
                    "state": {},
                    "workflowStatus": "DRAFT",
                },
            ),
        )

    def new_model_class(self, unique_name: str = "aModelClass") -> Dict[str, Any]:
        return self._conn.add_object(
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

    def new_empty_datamart(self, name: str, fields_spec: Optional[List[Dict]]):
        response = self._session.post(
            self.endpoint_url("datamart.newfc/DM"),
            json={"data": {"uniqueName": name, "label": name}},
        )
        entry = response.json()["response"]["data"][0]
        self._session.post(
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
) -> Dict[str, Dict[str, Any]]:
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
