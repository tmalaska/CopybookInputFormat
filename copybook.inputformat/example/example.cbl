       01 EBCDIC-CM-INIT .

           03 USER-ID PIC S9999.

           03 USER-NAME PIC S9(10).

           03 USER-AGE PIC XXX.

           03 CM00-INIT .

               05 CM00-CUS-BASE-INFO .

                   07 CM00-LAST-FOUR-SSN PIC S9999 COMP-5.

                   07 CM00-CUS-BASE-INFO-SUB .

                       09 CM00-RANDOM-NUM PIC S9(9).
                       
                       09 CM00-RANDOM-STRING PIC XXXXX.
