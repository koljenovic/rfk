FUNCTION MAIN(path, base, csvfile)
    LOCAL i
    Set(_SET_FILECASE, 2)
    IF PCount() < 3
        OutStd("400 ERROR. USAGE e.g: append ABS_DBPATH NAME.DBF ABSCSVFILEPATH [INDEX01.NTX ...]")
        RETURN 1
    ENDIF
    Set(_SET_DEFAULT, hb_DirSepToOS(path))
    USE (base)
    IF !FLock()
        OutStd("500 ERROR. LOCKED")
    ENDIF
    FOR i := 4 TO PCount()
        SET INDEX TO (hb_PValue(i)) ADDITIVE
        IF !FLock()
            OutStd("500 ERROR. LOCKED")
        ENDIF
    NEXT
    Set(_SET_FILECASE, 0)
    APPEND FROM (csvfile) DELIMITED
    OutStd("200 SUCCESS")
    RETURN 0
