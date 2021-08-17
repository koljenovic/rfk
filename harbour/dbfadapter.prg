#include "dbstruct.ch"

FUNCTION MAIN(command)
    LOCAL i, args := {}
    
    SET DELETED ON

    FOR i := 2 TO PCount()
        AAdd(args, hb_Pvalue(i))
    NEXT

    DO CASE
        CASE Upper(command) = "APPEND"
            APPEND()
        CASE Upper(command) = "EXPORT"
            EXPORT(args)
        CASE Upper(command) = "HEAD"
            HEAD(args)
    ENDCASE

FUNCTION HEAD(argv)
    LOCAL field, struct, path, base, tmpfile, output := {}
    IF Len(argv) < 3
        OutStd("400 ERROR. USAGE e.g: filter ABS_DBPATH NAME.DBF ABSTMPFILEPATH [INDEX01.NTX ...]")
        RETURN 1
    ENDIF
    path := argv[1]
    base := argv[2]
    tmpfile := argv[3]
    Set(_SET_FILECASE, 2)
    Set(_SET_DEFAULT, hb_DirSepToOS(path))

    USE (base)
    IF !FLock()
        OutStd("500 ERROR. LOCKED")
    ENDIF

    Set(_SET_FILECASE, 0)
    struct := dbStruct()
    FOR EACH field IN struct
        AAdd(output, '<' + allTrim(hb_ValToStr(field[DBS_NAME])) + ", " + allTrim(hb_ValToStr(field[DBS_TYPE])) + ", " + allTrim(hb_ValToStr(field[DBS_LEN])) + ", " + allTrim(hb_ValToStr(field[DBS_DEC])) + '>')
    NEXT
    hb_memoWrit(tmpfile, hb_jsonEncode(output))
    OutStd("200 SUCCESS")

FUNCTION EXPORT(argv)
    LOCAL i, path, base, tmpfile
    IF Len(argv) < 3
        OutStd("400 ERROR. USAGE e.g: filter ABS_DBPATH NAME.DBF ABSTMPFILEPATH [INDEX01.NTX ...]")
        RETURN 1
    ENDIF
    path := argv[1]
    base := argv[2]
    tmpfile := argv[3]
    Set(_SET_FILECASE, 2)
    Set(_SET_DEFAULT, hb_DirSepToOS(path))
    
    USE (base)
    IF !FLock()
        OutStd("500 ERROR. LOCKED")
    ENDIF
    FOR i := 4 TO Len(argv)
        SET INDEX TO (argv[i]) ADDITIVE
        IF !FLock()
            OutStd("500 ERROR. LOCKED")
        ENDIF
    NEXT
    Set(_SET_FILECASE, 0)
    COPY TO (tmpfile) DELIMITED
    OutStd("200 SUCCESS")

FUNCTION APPEND(argv)
    LOCAL i, path, base, csvfile
    Set(_SET_FILECASE, 2)
    IF Len(argv) < 3
        OutStd("400 ERROR. USAGE e.g: append ABS_DBPATH NAME.DBF ABSCSVFILEPATH [INDEX01.NTX ...]")
        RETURN 1
    ENDIF
    path := argv[1]
    base := argv[2]
    csvfile := argv[3]

    Set(_SET_DEFAULT, hb_DirSepToOS(path))
    USE (base)
    IF !FLock()
        OutStd("500 ERROR. LOCKED")
    ENDIF
    FOR i := 4 TO Len(argv)
        SET INDEX TO (argv[i]) ADDITIVE
        IF !FLock()
            OutStd("500 ERROR. LOCKED")
        ENDIF
    NEXT
    Set(_SET_FILECASE, 0)
    APPEND FROM (csvfile) DELIMITED
    OutStd("200 SUCCESS")

RETURN Nil