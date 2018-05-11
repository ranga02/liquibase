package aim.config;

import liquibase.changelog.IncludeAllFilter;

//@TODO : APPDBD - Filter for looking at only the DDL files with pattern: /dddd_DDL_*.sql
public class IncludeDdlFilesOnlyFilter implements IncludeAllFilter{

    @Override
    public boolean include(String changeLogPath) {
        if (changeLogPath.toLowerCase().matches(".*/*\\d{4}_ddl_.*\\.sql")) {
            return true;
        }
        return false;
    }

    public static void main(String[] args) {
        IncludeDdlFilesOnlyFilter i = new IncludeDdlFilesOnlyFilter();
        String s = "/0090_DDL_tbl.sql";
        System.out.println("i.include() = " + i.include(s));
    }
}
