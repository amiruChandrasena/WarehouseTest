using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class ReplenishItem
    {
        public string CatalogCode { get; set; }
        public DateTime BestBefore { get; set; }
        public int PickingSequence { get; set; }
        public string BinLocation { get; set; }
        public int PalletUnits { get; set; }
        public bool IsPick { get; set; }
        public string RoomType { get; set; } //Add By Irosh 2023/03/21
    }
}
