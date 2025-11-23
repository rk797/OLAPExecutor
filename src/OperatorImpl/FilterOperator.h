#pragma once

#include "Operator.h"

class FilterOperator : public Operator
{
public:
    enum class FilterMode
    {
        Avx,
        Scalar
    };

	// For our limmited example, we simplify the filter to a single integer comparison
	// If the value in the column is greater than FilterValue, we keep it
    FilterOperator(std::unique_ptr<Operator> Child, int FilterValue, FilterMode Mode);

    DataChunk Next() override;

private:
    DataChunk ApplyAvxFilter(const DataChunk& InputChunk);
    DataChunk ApplyScalarFilter(const DataChunk& InputChunk);
    FilterMode CurrentMode; // Can be AVX or Scalar
	std::unique_ptr<Operator> ChildOperator; // Typically a ScanOperator or MemoryScanOperator
	int ValueToCompare; // If x > ValueToCompare, keep x
};

