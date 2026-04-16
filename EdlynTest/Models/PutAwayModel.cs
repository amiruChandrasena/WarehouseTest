using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class PutAwayModel
    {
        public string Description { get; set; }
        public string CatalogCode { get; set; }
        public string RoomType { get; set; }
        public string CurrentBinLocation { get; set; }
        public bool IsMixedPallet { get; set; }
        public string OptionalRoomType { get; set; }
        public string SuggestedRack { get; set; }
        public int PickingSequence { get; set; }
        public int SetPalletUnits { get; set; } // for replenish
        public bool IsPick { get; set; }

        public PalletHeader Pallet { get; set; }
    }
}
