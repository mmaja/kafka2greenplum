package com.xhh.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by young on 2018/2/11.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Structure {
    private String database;
    private String table;
    private String type;
    private String ts;
    private String xid;
    private String commit;
    private String data;
    private String old;



}
