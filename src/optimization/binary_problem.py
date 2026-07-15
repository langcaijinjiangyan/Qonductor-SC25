import pymoo.gradient.toolbox as anp
from pymoo.core.problem import ElementwiseProblem


class BinarySchedulingProblem(ElementwiseProblem):
    """
    A problem for scheduling quantum jobs to quantum computers
    using binary variables
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
            n_var=num_jobs * num_backends,
            n_obj=2,
            n_ieq_constr=num_jobs,
            n_eq_constr=num_jobs,
            xl=0.0,
            xu=1.0,
            **kwargs,
        )

    def _evaluate(self, x: anp.ndarray, out: dict, *args, **kwargs):
        """
        Evaluate the problem
        :param x: Solution to be evaluated, must be a 1D array of shape
        (num_jobs * num_backends)
        :param out: Results of the evaluation
        """
        # Reshape the solutions to a 2D array of shape
        # (num_jobs, num_backends)
        assignments = x.reshape(self.num_jobs, self.num_backends)

        # Calculate the objective values
        f1 = self._calculate_mean_waiting_time(assignments)
        f2 = self._calculate_mean_error(assignments)
        # Calculate the inequality constraint values
        g = self._calculate_backend_size_constraint(assignments)
        # Calculate the equality constraint values
        h = self._calculate_job_assignment_constraint(assignments)

        # Assign the results to the output dictionary
        out["F"] = [f1, f2]
        out["G"] = g
        out["H"] = h

    def _calculate_mean_waiting_time(
        self, assignments: anp.ndarray
    ) -> anp.ndarray:
        """
        Calculate the geometric mean waiting time of the jobs on the backends
        :param assignments: Assignments of jobs to backends, must be a 2D
        array of shape (num_jobs, num_backends)
        :return: Geometric mean waiting time of the jobs on the backends
        """
        # Get the execution times of jobs on backends
        execution_times = anp.sum(self.execution_times * assignments, axis=0)
        # Calculate the waiting times of job queues on backends, including
        # the execution times of the current jobs
        waiting_times = anp.where(
            execution_times > 0, execution_times + self.waiting_times, 0
        )

        return anp.mean(waiting_times[anp.nonzero(waiting_times)])

    def _calculate_mean_error(self, assignments: anp.ndarray) -> anp.ndarray:
        """
        Calculate the geometric mean error of the jobs on the backends
        :param assignments: Assignments of jobs to backends, must be a 2D
        array of shape (num_jobs, num_backends)
        :return: Median error of the jobs on the backends
        """
        # Get the fidelities of the assigned jobs
        fidelities = anp.sum(self.fidelities * assignments, axis=1)
        # Calculate the errors of jobs on backends
        errors = 1 - fidelities

        return anp.mean(errors, axis=0)

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
        backend_sizes = anp.sum(self.backend_sizes * assignments, axis=1)

        return self.job_sizes - backend_sizes

    @staticmethod
    def _calculate_job_assignment_constraint(
        assignments: anp.ndarray,
    ) -> anp.ndarray:
        """
        Calculate the constraint values of the job assignment constraint
        :param assignments: Assignments of jobs to backends, must be a 2D
        array of shape (num_jobs, num_backends)
        :return: Constraint values of the job assignment constraint
        """
        return anp.sum(assignments, axis=1) - 1
