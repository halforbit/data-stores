using Halforbit.DataStores.Validation.Model;
using System;

namespace Halforbit.DataStores.Validation.Exceptions
{
    public class ValidationException : Exception
    {
        public ValidationException(
            ValidationErrors validationErrors)
        {
            ValidationErrors = validationErrors ?? ValidationErrors.Empty;
        }

        public override string Message => $"Validation error(s) occurred: {ValidationErrors}";

        public ValidationErrors ValidationErrors { get; }
    }
}
