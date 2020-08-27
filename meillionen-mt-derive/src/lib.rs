use proc_macro::TokenStream;
use quote::quote;
use syn;
use syn::Data;

fn build_df_conversion(ident: &syn::Ident, ty: &syn::Type) -> proc_macro2::TokenStream {
    let fieldname = ident.to_string();
    let gen = quote! {
        #ident: obj.get_item(#fieldname)?
            .call_method0("to_numpy")?
            .extract::<&PyArray1<#ty>>()?
            .to_owned_array()
    };
    gen
}

fn extract_type_param(ty: &syn::Type) -> &syn::Type {
    match ty {
        syn::Type::Path(ref path) => {
            return path.path.segments.last().and_then(|s| match s.arguments {
                syn::PathArguments::AngleBracketed(ref params) => {
                    params.args.first()
                }
                _ => panic!("type does not have generic parameters")
            }).map(|ga| match ga {
                syn::GenericArgument::Type(ref ty) => ty,
                _ => panic!("type must be syn::GenericArgument::Type")
            }).unwrap()
        },
        _ => panic!("type must be a syn::Type::Path")
    };
}

fn impl_from_pandas(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let mut field_exprs = Vec::new();
    if let Data::Struct(ref struct_data) = &ast.data {
        for field in struct_data.fields.iter() {
            let ident = field.ident.as_ref().unwrap();
            let ty = &field.ty; // Array1<f64>
            let inner_ty = extract_type_param(ty);
            field_exprs.push(build_df_conversion(ident, inner_ty));
        }
    }
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let gen = quote! {
        impl #impl_generics FromPandas for #name #ty_generics #where_clause {
            fn from_pandas(obj: &PyAny) -> Result<Self, PyErr> {
                Ok(Self {
                    #(#field_exprs),*
                })
            }
        }
    };
    gen.into()
}

#[proc_macro_derive(FromPandas)]
pub fn from_pandas_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    impl_from_pandas(&ast)
}

fn impl_into_pandas(ast: &syn::DeriveInput) -> TokenStream {
    let mut field_set_exprs = Vec::new();
    if let syn::Data::Struct(ref struct_data) = &ast.data {
        for field in struct_data.fields.iter() {
            let fieldname = field.ident.as_ref().unwrap();
            let colname = fieldname.to_string();
            let field_set_expr = quote! {
                kwargs.set_item(#colname, self.#fieldname.into_pyarray(py))?;
            };
            field_set_exprs.push(field_set_expr)
        }
    }

    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();
    let gen = quote! {
        impl #impl_generics IntoPandas for #name #ty_generics #where_clause {
            fn into_pandas(self, py: pyo3::Python) -> pyo3::PyResult<&pyo3::types::PyAny> {
                let pandas = pyo3::types::PyModule::import(py, "pandas")?;
                let kwargs = pyo3::types::PyDict::new(py);
                #(#field_set_exprs);*
                pandas.call1("DataFrame", (kwargs,))
            }
        }
    };
    gen.into()
}

#[proc_macro_derive(IntoPandas)]
pub fn into_pandas_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    impl_into_pandas(&ast)
}

#[cfg(test)]
mod test {}