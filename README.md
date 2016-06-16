Worker
---

The dedicated Optimization Problem solver.

### About

The primary role `worker` is solving the optimization problem. The inherent complexity is encapsulated here so that the webserver and other collaborating systems are shielded from it. Communication with other systems is made possible through a messaging interface ( `worker` talks directly to database for the time being, but ideally this may change). This architectural style has the following benefits:

 - Simplifies provisioning of collaborating systems
 - `worker` is flexible and language-agnostic
 - Simple message-oriented interface

### Development Tools

`dev` and `feat/*` branches are encouraged to take advantage of `jupyter` for prototyping solutions to technical problems. Notebook checkpoint data is ignored from version control and a `notebooks` folder in the the project root exist for exactly this purpose.

### Extending

Naturally (assuming the Anaconda stack is present) other tools may be used to _solve_ the optimization problem. `R` is easily provisioned using the normal anaconda methods. There exist actively maintained solutions to integrate `python` and `R` such as `rpy2` and `Rserve`.

see [Combining the powerful worlds of Python and R](https://www.youtube.com/watch?v=ucJ2-5a2CAA) as a starting point

### Troubleshooting

Curious about the clock time but you don't want to sit and watch a terminal tick? Easy:

```time ( python src/optimizer.py )```
