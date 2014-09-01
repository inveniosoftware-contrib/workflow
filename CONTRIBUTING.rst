Contributing
============

Bug reports, feature requests, and other contributions are welcome.
If you find a demonstrable problem that is caused by the code of this
library, please:

1. Search for `already reported problems
   <https://github.com/inveniosoftware/workflow/issues>`_.
2. Check if the issue has been fixed or is still reproducible on the
   latest `master` branch.
3. Create an issue with **a test case**.

If you create a feature branch, you can run the tests to ensure everything is
operating correctly:

.. code-block:: console

    $ ./run-tests.sh

    ...
    Name                            Stmts   Miss  Cover   Missing
    -------------------------------------------------------------
    workflow/__init__                   2      0   100%
    workflow/config                   231     92    60%   ...
    workflow/engine                   321     93    71%   ...
    workflow/patterns/__init__          5      0   100%
    workflow/patterns/controlflow     159     66    58%   ...
    workflow/patterns/utils           249    200    20%   ...
    workflow/version                    2      0   100%
    -------------------------------------------------------------
    TOTAL                             969    451    53%

    ...

    55 passed, 1 warnings in 3.10 seconds
