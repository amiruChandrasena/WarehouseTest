using Abstractions.ServiceInterfaces;
using Models;
using Models.Dto;
using System;
using System.Collections.Generic;
using System.Text;
using System.Transactions;

namespace Business
{
    public class CountPickBusiness
    {
        private readonly ICountPickService _countPickService;

        public CountPickBusiness(ICountPickService countPickService)
        {
            _countPickService = countPickService;
        }
        /*
        public TransactionWrapper Correct(CountPickDto countPickDto)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (TransactionScope scope = new TransactionScope())
            {
                wrapper = _countPickService.UpdateStockCountStatus(countPickDto.BinLocation);
                if (wrapper.IsSuccess == false)
                {
                    scope.Dispose();
                    return wrapper;
                }

                List<CycleCountPallet> stockPallets = new List<CycleCountPallet>();

                for (int i = 0; i < countPickDto.PalletLabels.Count; i++)
                {
                    CycleCountPallet stockPallet = new CycleCountPallet();
                    stockPallet.BestBefore = countPickDto.PalletLabels[i].BestBefore;
                    stockPallet.CatalogCode = countPickDto.PalletLabels[i].CatalogCode;
                    stockPallet.PalletNumber = countPickDto.PalletNumber;
                    stockPallet.PalletUnits = countPickDto.PalletLabels[i].PalletUnits;
                    stockPallets.Add(stockPallet);
                }

                wrapper = _countPickService.InsertStockCount(countPickDto.BinLocation, countPickDto.Originator, stockPallets);
                if (wrapper.IsSuccess == false)
                {
                    scope.Dispose();
                    return wrapper;
                }

                scope.Complete();
            }

            PalletLocationLog palletLocationLog = new PalletLocationLog
            {
                Timestamp = DateTime.Now,
                PalletNo = countPickDto.PalletNumber,
                NewLocation = countPickDto.BinLocation,
                MovedBy = countPickDto.Originator,
                Remark = "PICK COUNT CORRECT",
                SyncTime = ""
            };

            wrapper.IsSuccess = _countPickService.InsertPalletLocationLog(palletLocationLog); // no need to hold if fails, not essential data

            wrapper.IsSuccess = true;
            return wrapper;
        } */

        public TransactionWrapper GetPickLocationData(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            try
            {
                wrapper = _countPickService.GetPickLocationDetail(binLocation);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                CountPickDto countPickDto = wrapper.ResultSet[0] as CountPickDto;

                wrapper = _countPickService.GetPalletLabelModels(countPickDto.PalletNumber);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                countPickDto.PalletLabels = wrapper.ResultSet[0] as List<PalletLabelModel>;
                wrapper.ResultSet.Clear();

                int unitsBeforeChange = 0;

                for (int i = 0; i < countPickDto.PalletLabels.Count; i++)
                {
                    unitsBeforeChange += countPickDto.PalletLabels[i].PalletUnits;
                    countPickDto.PalletLabels[i].OriginalPalletUnits = countPickDto.PalletLabels[i].PalletUnits;
                    countPickDto.WarehouseId = countPickDto.PalletLabels[i].WarehouseId;
                    countPickDto.Description = countPickDto.PalletLabels[i].Description;
                }

                countPickDto.PalletUnits = unitsBeforeChange;
                countPickDto.UnitsBeforeChange = unitsBeforeChange;
                countPickDto.Narration = "PICK BIN COUNT";

                wrapper.IsSuccess = true;
                wrapper.ResultSet.Add(countPickDto);
                return wrapper;
            } catch (Exception e)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add(e.Message);
                return wrapper;
            }
        }

        public TransactionWrapper Save(CountPickDto countPickDto)
        {
            /*
            countPickDto.PalletUnits = 0;

            for (int i = 0; i < countPickDto.PalletLabels.Count; i++)
            {
                countPickDto.PalletUnits += countPickDto.PalletLabels[i].PalletUnits;
                if (String.IsNullOrEmpty(countPickDto.PalletLabels[i].CatalogCode))
                {
                    countPickDto.PalletLabels[i].CatalogCode = countPickDto.CatalogCode;
                }
            }

            countPickDto.AdjustedQuantity = countPickDto.PalletUnits - countPickDto.UnitsBeforeChange;
            countPickDto.MovementDate = DateTime.Now; */

            TransactionWrapper wrapper = new TransactionWrapper();

            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    wrapper = _countPickService.UpdateStockCountStatus(countPickDto.BinLocation);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    List<CycleCountPallet> stockPallets = new List<CycleCountPallet>();

                    for (int i = 0; i < countPickDto.PalletLabels.Count; i++)
                    {
                        CycleCountPallet stockPallet = new CycleCountPallet();
                        stockPallet.BestBefore = countPickDto.PalletLabels[i].BestBefore;
                        stockPallet.CatalogCode = countPickDto.PalletLabels[i].CatalogCode;
                        stockPallet.PalletNumber = countPickDto.PalletNumber;
                        stockPallet.PalletUnits = countPickDto.PalletLabels[i].PalletUnits;
                        stockPallets.Add(stockPallet);
                    }

                    wrapper = _countPickService.InsertStockCount(countPickDto.BinLocation, countPickDto.Originator, stockPallets);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    wrapper = _countPickService.UpdatePalletHeaderNotInLocation(countPickDto.PalletNumber, countPickDto.BinLocation);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    wrapper = _countPickService.UpdateWarehouseConfigUnits(countPickDto.PalletUnits, countPickDto.BinLocation);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    wrapper = _countPickService.DeletePalletDetail(countPickDto.PalletNumber);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    for (int i = 0; i < countPickDto.PalletLabels.Count; i++)
                    {
                        wrapper = _countPickService.InsertPalletDetail(countPickDto.PalletLabels[i], countPickDto.PalletNumber, countPickDto.WarehouseId);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }
                    }

                    string status = "";
                    if (!String.IsNullOrEmpty(countPickDto.BinLocation))
                    {
                        status = "W";
                    }
                    else
                    {
                        status = "P";
                    }

                    string reason = "Pick Phase ADJ";
                    wrapper = _countPickService.InsertPalletMovement(countPickDto.PalletNumber, reason, countPickDto.Originator, status);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    PalletLocationLog palletLocationLog = new PalletLocationLog
                    {
                        MovedBy = countPickDto.Originator,
                        NewLocation = countPickDto.BinLocation,
                        PalletNo = countPickDto.PalletNumber,
                        SyncTime = "",
                        Timestamp = DateTime.Now,
                        ManifestNo = 0,
                        Remark = "PICK BIN COUNT " + countPickDto.UnitsBeforeChange.ToString() + " to " + countPickDto.PalletUnits.ToString()
                    };

                    wrapper.IsSuccess = _countPickService.InsertPalletLocationLog(palletLocationLog); // no need to hold up if this fails, is not essential data
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    scope.Complete();
                    wrapper.IsSuccess = true;
                }
                catch (Exception e)
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }

            return wrapper;
        }
    }
}
