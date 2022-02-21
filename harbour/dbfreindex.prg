FUNCTION MAIN(path, file)
    LOCAL i
    Set(_SET_FILECASE, 2)
    IF PCount() < 2
        OutStd("400 ERROR. USAGE e.g: reindex ABS_PATH NAME.DBF [INDEX01.NTX ...]")
        RETURN 1
    ENDIF
    Set(_SET_DEFAULT, hb_DirSepToOS(path))
    USE (file)
    IF !FLock()
        OutStd("500 ERROR. LOCKED")
    ENDIF
    FOR i := 3 TO PCount()
        SET INDEX TO (hb_PValue(i)) ADDITIVE
        IF !FLock()
            OutStd("500 ERROR. LOCKED")
        ENDIF
    NEXT
    REINDEX
    OutStd("200 SUCCESS")
    RETURN 0