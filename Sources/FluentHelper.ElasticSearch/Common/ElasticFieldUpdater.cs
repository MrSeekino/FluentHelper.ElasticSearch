using FluentHelper.ElasticSearch.Interfaces;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq.Expressions;

namespace FluentHelper.ElasticSearch.Common
{
    internal sealed class ElasticFieldUpdater<TEntity> : IElasticFieldUpdater<TEntity> where TEntity : class
    {
        bool UpdateAllFieldsCalled { get; set; }

        string FieldIdName { get; set; }
        List<string> FieldList { get; set; }

        public ElasticFieldUpdater(string fieldIdName)
        {
            UpdateAllFieldsCalled = false;
            FieldIdName = fieldIdName;
            FieldList = new List<string>();
        }

        public List<string> GetFieldList()
        {
            return FieldList;
        }

        public IElasticFieldUpdater<TEntity> Update<TProperty>(Expression<Func<TEntity, TProperty>> expression)
        {
            if (UpdateAllFieldsCalled)
                throw new InvalidOperationException("Cannot call 'Update' after 'UpdateAllFields'");

            var memberExpressionBody = (MemberExpression)expression.Body;
            string fieldName = memberExpressionBody.Member.Name;

            if (fieldName != FieldIdName)
                FieldList.Add(fieldName);

            return this;
        }

        public IElasticFieldUpdater<TEntity> UpdateAllFields()
        {
            UpdateAllFieldsCalled = true;

            FieldList.Clear();
            foreach (PropertyDescriptor property in TypeDescriptor.GetProperties(typeof(TEntity)))
                if (property.Name != FieldIdName)
                    FieldList.Add(property.Name);

            return this;
        }
    }
}
