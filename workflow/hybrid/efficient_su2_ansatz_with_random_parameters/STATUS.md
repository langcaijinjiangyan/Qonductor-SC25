# Current status

The OpenQASM 3 parameterized circuit and 12-qubit directory
structure are ready.

The copied C program is still the legacy implementation and must
not be used for production submission until it is updated to:

- send the complete named parameter set;
- call the parameterized q_exec API;
- parse the QOS counts object correctly;
- use quantum results in the classical update;
- use an explicit EXECUTE/STOP protocol.
