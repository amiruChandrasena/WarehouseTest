using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class MoveToRackDto
    {
        public string WarehouseCode { get; set; }
        public string RoomCode { get; set; }
        public string RackCode { get; set; }
        public string Originator { get; set; }
        public List<PutAwayModel> Pallets { get; set; }

        public MoveToRackDto()
        {
            Pallets = new List<PutAwayModel>();
        }
    }
}
