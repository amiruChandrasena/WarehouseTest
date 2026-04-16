using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class PalletLocationLog
    {
        public DateTime Timestamp { get; set; }
        public int PalletNo { get; set; }
        public string NewLocation { get; set; }
        public string MovedBy { get; set; }
        public string SyncTime { get; set; }
        public int ManifestNo { get; set; }
        public string Remark { get; set; }
    }
}
