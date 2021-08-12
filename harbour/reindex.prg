FUNCTION MAIN(path, file)
    LOCAL i
    Set(_SET_FILECASE, 2)
    IF PCount() < 2
        OutStd("400 ERROR. USAGE e.g: reindex ABS_PATH NAME.DBF [INDEX01.NTX ...]")
        RETURN 1
    ENDIF
    Set(_SET_DEFAULT, hb_DirSepToOS(path))
    USE (file)
    FOR i := 3 TO PCount()
        SET INDEX TO (hb_PValue(i)) ADDITIVE
    NEXT
    REINDEX
    OutStd("200 SUCCESS")
    RETURN 0