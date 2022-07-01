public class Constants {
    public static final String Directory = System.getProperty("user.home") + "/.ipls/";
    public static class Discovery {
        public static final String Topic = "IPFSDISCOVERY";
        public static final int Wait = 1000; //ms
    }
    public static class MessageTags {
        public static final byte Discover = 0;
        public static final byte DiscoverReply = 1;
        public static final byte Merge_Request = 5;
        public static final byte SendPartition = 2;
        public static final byte SendPartitionReply = 18;
        public static final byte WantPartition = 3;
        public static final byte PartitionReply = 4;
        public static final byte Replication_Request = 6;
        public static final byte ReplicationReply = 7;
    }
}
