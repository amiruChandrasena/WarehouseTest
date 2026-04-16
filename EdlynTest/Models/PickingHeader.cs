using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class PickingHeader
    {
        public PickingHeader()
        {
            PicklistItems = new List<PicklistItem>();
            PickedItems = new List<ManifestLoadingStatus>();
            PickingNotes = new List<PickerNote>();
        }

        public int PicklistNumber { get; set; }
        public Manifest Manifest { get; set; }
        public string CustomerName { get; set; }
        public int OpenPalletNumber { get; set; }
        public int NegativePickBin { get; set; }
        public Carrier Carrier { get; set; }
        public List<PicklistItem> PicklistItems { get; set; }
        public List<ManifestLoadingStatus> PickedItems { get; set; }
        public List<PickerNote> PickingNotes { get; set; }
    }
}
