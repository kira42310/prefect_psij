from __future__ import annotations

# import asyncio
from contextlib import ExitStack
from typing import (
    # TYPE_CHECKING,
    Any,
    # Callable,
    Coroutine,
    Iterable,
    Optional,
    Set,
    TypeVar,
    Union,
    overload,
)

from typing_extensions import ParamSpec

from prefect.client.schemas.objects import State, TaskRunInput
#from prefect.futures import PrefectFuture, PrefectFutureList, PrefectWrappedFuture
from prefect.futures import PrefectFuture, PrefectWrappedFuture
from prefect.logging.loggers import get_logger, get_run_logger
from prefect.task_runners import TaskRunner
from prefect.tasks import Task
# from prefect.utilities.asyncutils import run_coro_as_sync
# from prefect.utilities.collections import visit_collection
# from prefect.utilities.importtools import from_qualified_name, to_qualified_name, lazy_import
from datetime import timedelta

import psij_ext
import psij
import pickle

logger = get_logger(__name__)

P = ParamSpec("P")
T = TypeVar("T")
F = TypeVar("F", bound=PrefectFuture[Any])
R = TypeVar("R")

class PrefectPSIJFuture(PrefectWrappedFuture[R, psij.Job]):

    # This function will wait the PSI/J query the job scheduler and get the complete status 
    # Parameters
    # - timeout, float, Optional, set the timeout time
    # Return: no return 
    def wait(self, timeout: Optional[float] = None ) -> None:
        try:
            result = self.wrapped_future.wait( )
        except Exception:
            return 'Something go wrong => todo****'
        # if isinstance(result, State):
        #     self._final_state = result
        self._final_state = result
    
    # This function will return the results from the job by callling _get_results function, if the _final_state has value. If the _final_state does not has value, the wait function will be invoke and wait until the PSI/J get the complete status from the job scheduler and wall _get_restuls function
    # parameters:
    # - timeout, float, Optional, set the timeout time
    # - raise_on_failure, bool, default = True, Not use in this implemenatation
    # Return: Results and error messages from the job
    def result( self, timeout: Optional[float] = None, raise_on_failure: bool = True ) -> R:
        if not self._final_state:
            try:
                result_state = self.wrapped_future.wait( )
            # except distributed.TimeoutError as exc:
            #     raise TimeoutError( f"Task run {self.task_run_id} did not complete within {timeout} seconds" ) from exc
            except Exception: # PSI/J dese not have timeout exceptiond
                return "Timeout Error"
            
            # if isinstance( future_result, State ):
            #     self._final_state = future_result
            # else:
            #     return future_result
        #return self._get_results( self.wrapped_future.spec.attributes.custom_attributes['tmp_output'] )
        output = self._get_results( self.wrapped_future.spec.attributes.custom_attributes['tmp_output'] )
        if output[1] is not None:
            raise output[1]
        return output[0]


    # This function will receive the output file location, use pickle to deserialize the files, and return the results and error messages in dict(key, value) data type. Keys "results" for results from the job, "errors" for error messages
    # parameters:
    # output_file_location, [str, Path], the output file location 
    # Return: dict[results, error messages]
    def _get_results( self, output_file_location: Union[str, Path] ):
        with open( output_file_location, 'rb' ) as f:
            output = pickle.load( f )
        #return { 'results': output[0], 'errors': output[1] }
        # output[0] is results, output[1] is exception
        return output 

class PSIJTaskRunner(TaskRunner):

    # Init of the class, will get the job scheduler executor from the PSI/J by instance parameter, get the job specification, and if need change the work directory
    # parameters:
    # - instance, str, job scheduler name
    # - job_spec, Dict[ str, object ], The job specification parameter can be check at the PSI/J documents
    # - work_directory, [str, Path], Optional, For change the location to write PSI/J files( job submission script, python execution script, data file, output, ... )
    # Return: no return
    def __init__(self, instance: str, job_spec: Dict[str,object], work_directory: Union[str, Path, None] = None, keep_files = False ):

        self.instance = instance
        self.job_spec = job_spec
        self.job_executor = psij_ext.psij_ext(self.instance, keep_files=keep_files)
        if work_directory is not None:
            self.job_executor.work_directory = work_directory
        
        self._exit_stack = ExitStack()

        super().__init__()

    # Comparison function 
    # parameters:
    # - other, object, another PSIJTaskRunner 
    # Return: bool
    def __eq__(self, other: object) -> bool:
        if isinstance(other, PSIJTaskRunner):
            return (
                self.instance == other.instance and
                self.job_spec == other.job_spec
            )
        else:
            return False

    # Create new/copy PSIJTaskRunner
    # parameters: None
    # Return: self
    def duplicate(self):
        return type(self)(
            instance = self.instance,
            job_spec = self.job_spec,
            work_directory = self.job_executor.work_directory
        )

    @overload
    def submit(
        self,
        task: "Task[P, Coroutine[Any, Any, R]]",
        parameters: dict[str, Any],
        wait_for: Iterable[PrefectPSIJFuture[R]] | None = None,
        dependencies: dict[str, Set[TaskRunInput]] | None = None,
    ) -> PrefectPSIJFuture[R]: ...

    @overload
    def submit(
        self,
        task: "Task[Any, R]",
        parameters: dict[str, Any],
        wait_for: Iterable[PrefectPSIJFuture[R]] | None = None,
        dependencies: dict[str, Set[TaskRunInput]] | None = None,
    ) -> PrefectPSIJFuture[R]: ...

    # Job submission function, will call executor submit_python function, create job, python execution script, serialize data, and submit the job
    # parameters:
    # - task, Provide by prefect, task.fn provide the python function object
    # - parameters, Parameters of the task.fn
    # - wait_for, ?
    # - dependencies, ?
    # Return: PrefectPSIJFuture[ task_id, psij.Job ]
    def submit(
        self,
        task: "Task[P, R]",
        parameters: dict[str, Any],
        wait_for: Iterable[PrefectPSIJFuture[R]] | None = None,
        dependencies: dict[str, Set[TaskRunInput]] | None = None,
    ) -> PrefectPSIJFuture[R]:

        job = self.job_executor.submit_python( task.fn, self.job_spec, kwargs=parameters )
        if not isinstance(job.spec.attributes.custom_attributes, dict):
            job.spec.attributes.custom_attributes = {}
        job.spec.attributes.custom_attributes['tmp_output'] = f'{self.job_executor.work_directory}/{job.id}_out.pkl'

        return PrefectPSIJFuture[R]( task_run_id=job.id, wrapped_future=job )

    # Not sure this PSI/J need this function, we can use the Job scheduler array job to do it(Use MPI to distribute job across Cluster, especially Fugaku with Tofu network topology and MPI tuned for Tofu network)
    # @overload
    # def map(
    #     self,
    #     task: "Task[P, Coroutine[Any, Any, R]]",
    #     parameters: dict[str, Any],
    #     wait_for: Iterable[PrefectFuture[Any]] | None = None,
    # ) -> PrefectFutureList[PrefectPSIJFuture[R]]: ...

    # @overload
    # def map(
    #     self,
    #     task: "Task[P, R]",
    #     parameters: dict[str, Any],
    #     wait_for: Iterable[PrefectFuture[Any]] | None = None,
    # ) -> PrefectFutureList[PrefectPSIJFuture[R]]: ...

    # def map(
    #     self,
    #     task: "Task[P, R]",
    #     parameters: dict[str, Any],
    #     wait_for: Iterable[PrefectFuture[Any]] | None = None,
    # ):
    #     return super().map(task, parameters, wait_for)

    def __enter__(self):
        """
        Start the task runner and create an exit stack to manage shutdown.
        """
        super().__enter__()
        self._exit_stack.__enter__()

        return self

    def __exit__(self, *args: Any) -> None:
        self._exit_stack.__exit__(*args)
        super().__exit__(*args)
