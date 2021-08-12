FUNCTION MAIN(path, file)
    // LOCAL GetList := {}
    // LOCAL mT
    LOCAL i
    Set(_SET_FILECASE, 2)
    IF PCount() < 2
        OutStd("400 ERROR. USAGE e.g: reindex ABS_PATH NAME.DBF [INDEX01.NTX ...]")
        RETURN 1
    ENDIF
    // OutStd( path, hb_eol() )
    // OutStd( file, hb_eol() )
    // SET DEFAULT TO /home/koljenovic/.rfk/data/ 
    Set(_SET_DEFAULT, hb_DirSepToOS(path))
    USE (file)
    FOR i := 3 TO PCount()
        OutStd(hb_PValue(i), hb_eol())
        SET INDEX TO (hb_PValue(i)) ADDITIVE
    NEXT
    REINDEX
    OutStd("200 SUCCESS")
    RETURN 0