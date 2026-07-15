import pymoo.gradient.toolbox as anp
from pymoo.core.problem import ElementwiseProblem


class DiscreteSchedulingProblem(ElementwiseProblem):
    """
    A problem for scheduling quantum jobs to quantum computers
    using discrete variables
    """

    def __init__(
        self,
        num_jobs: int,
        num_backends: int,
        execution_times: anp.ndarray,
        fidelities: anp.ndarray,
        waiting_times: anp.ndarray,
        job_sizes: anp.ndarray,
        backend_sizes: anp.ndarray,
        **kwargs,
    ):
        """
        Initialize the scheduling problem
        :param num_jobs: Number of jobs to be scheduled
        :param num_backends: Number of available backends
        :param execution_times: Execution times of jobs on backends, must
        be a 2D array of shape (num_jobs, num_backends)
        :param fidelities: Fidelities of jobs on backends, must be a 2D
        array of shape (num_jobs, num_backends)
        :param waiting_times: Waiting times of job queues on backends, must be
        a 1D array of shape (num_backends)
        :param job_sizes: Number of qubits of jobs, must be a 1D array
        of shape (num_jobs)
        :param backend_sizes: Sizes of backends, must be a 1D array of shape
        (num_backends)
        """
        self.num_jobs = num_jobs
        self.num_backends = num_backends
        self.execution_times = execution_times
        self.fidelities = fidelities
        self.waiting_times = waiting_times
        self.job_sizes = job_sizes
        self.backend_sizes = backend_sizes

        super().__init__(
            n_var=num_jobs,
            n_obj=2,
            n_ieq_constr=num_jobs,
            xl=0,
            xu=num_backends - 1,
            vtypes=int,
            **kwargs,
        )

    def _evaluate(self, x: anp.ndarray, out: dict, *args, **kwargs):
        """
        Evaluate the problem
        :param x: Solution to be evaluated, must be a 1D array of shape
        (num_jobs)
        :param out: Results of the evaluation
        """
        # Calculate the objective values
        f1 = self._calculate_mean_waiting_time(x)
        f2 = self._calculate_mean_error(x)
        # Calculate the inequality constraint values
        g = self._calculate_backend_size_constraint(x)

        # Assign the results to the output dictionary
        out["F"] = [f1, f2]
        out["G"] = g

    def _calculate_mean_waiting_time(
        self, assignments: anp.ndarray
    ) -> anp.ndarray:
        """
        Calculate the arithmetic mean waiting time of the jobs on the backends
        :param assignments: Assignments of jobs to backends, must be a 1D
        array of shape (num_jobs)
        :return: Arithmetic mean waiting time of the jobs on the backends
        """
        assignment_indices = anp.arange(self.num_jobs)
        # Get the execution times of jobs on backends
        execution_times = anp.bincount(
            assignments,
            weights=self.execution_times[assignment_indices, assignments],
            minlength=self.num_backends,
        )
        # Calculate the waiting times of job queues on backends, including
        # the execution times of the current jobs
        waiting_times = anp.where(
            execution_times > 0, execution_times + self.waiting_times, 0
        )

        return anp.mean(waiting_times[assignments])

    def _calculate_mean_error(self, assignments: anp.ndarray) -> anp.ndarray:
        """
        Calculate the arithmetic mean error of the jobs on the backends
        :param assignments: Assignments of jobs to backends, must be a 2D
        array of shape (num_jobs, num_backends)
        :return: Arithmetic mean error of the jobs on the backends
        """
        assignment_indices = anp.arange(self.num_jobs)

        # Get the fidelities of the assigned jobs
        fidelities = self.fidelities[assignment_indices, assignments]

        # Calculate the errors of jobs on backends
        errors = 1 - fidelities

        return anp.mean(errors)

    def _calculate_backend_size_constraint(
        self, assignments: anp.ndarray
    ) -> anp.ndarray:
        """
        Calculate the constraint values of the backend size constraint
        :param assignments: Assignments of jobs to backends, must be a 2D
        array of shape (num_jobs, num_backends)
        :return: Constraint values of the backend size constraint
        """
        # Get the sizes of the assigned backends
        backend_sizes = self.backend_sizes[assignments]

        return self.job_sizes - backend_sizes
