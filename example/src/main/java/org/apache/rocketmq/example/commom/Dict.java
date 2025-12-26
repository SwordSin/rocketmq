package org.apache.rocketmq.example.commom;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.remoting.RPCHook;

public class Dict {

    //
    public static String nameserver = "182.92.67.50:9876";

    public static RPCHook getRPCHook() {
        return new AclClientRPCHook(new SessionCredentials("handleClient", "Y22mVFW3uhVMYsZy6m28"));
    }

}
